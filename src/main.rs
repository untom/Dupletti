use anyhow::Result;
use blake2::{Blake2b, Digest};
use glob::glob;
use log;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Instant;
use structopt::StructOpt;

mod database;
pub use crate::database::{Database, FileDigest};

mod interface;
pub use crate::interface::*;

mod similarities;
pub use crate::similarities::*;

/// Search for duplicate files
#[derive(StructOpt, Debug)]
struct ProgramArguments {
    /// The pattern to look for
    #[structopt(short, long)]
    reset_database: bool,

    /// Whether to remove files from the DB that are not found in path
    #[structopt(short, long)]
    clean_unfound: bool,

    /// Number of threads for parallel processing (1 = single-threaded)
    #[structopt(short, long, default_value = "4")]
    threads: usize,

    /// The path to the file to read
    #[structopt(short, long, parse(from_os_str), default_value = "")]
    path: PathBuf,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    /// Use web interface or not.
    #[structopt(long)]
    no_web: bool,

    /// Binding address of the webinterface
    #[structopt(long, short, default_value = "127.0.0.1")]
    bind_address: String,

    /// Port of the web-interface
    #[structopt(long, default_value = "5757")]
    port: u16,

    /// Database commit batch size
    #[structopt(long, default_value = "1024")]
    commit_batchsize: usize,

    /// Allows web interface to serve files through preview links.
    /// Otherwise file links will be local and use file:// , which
    /// is not the best UX. However, this opens up a potential
    /// security risk, because it allows access random files from
    /// your disk through the web interface.
    /// It's recommended to only use this if you bind to an internal
    /// interface like 127.0.0.1.
    #[structopt(long)]
    allow_preview: bool,
}

fn list_files_in_directory<P: AsRef<Path>>(directory: P) -> HashSet<PathBuf> {
    let mut files = HashSet::new();
    let globresult = glob(directory.as_ref().join("**/*").to_str().unwrap()).unwrap();
    for entry in globresult {
        if let Ok(path) = entry {
            if path.is_file() {
                files.insert(path);
            }
        }
    }
    return files;
}

fn get_hash<D: Digest + Default>(filepath: &Path) -> io::Result<Vec<u8>> {
    let mut reader = fs::File::open(filepath)?;
    const BUFFER_SIZE: usize = 1024;
    let mut sh = D::default();
    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let n = reader.read(&mut buffer).unwrap();
        sh.update(&buffer[..n]);
        if n == 0 || n < BUFFER_SIZE {
            break;
        }
    }

    Ok(sh.finalize().to_vec())
}

fn remove_outdated_files(db: &Database, current_filelist: &HashSet<PathBuf>) -> Result<()> {
    let files_in_db = db.get_all_filedigests()?;
    for f in files_in_db {
        if !current_filelist.contains(&f.path) {
            println!("Removing {:?}", f.path);
            db.delete_filedigest(f.id)?;
        }
    }
    Ok(())
}

fn filter_out_files_already_in_database(
    db: &Database,
    current_filelist: HashSet<PathBuf>,
) -> Result<HashSet<PathBuf>> {
    let files_in_db = db.get_all_filedigests()?;
    let filepaths_in_db: HashSet<_> = files_in_db.iter().map(|f| &f.path).collect();
    let mut result = HashSet::<PathBuf>::new();
    for f in current_filelist {
        if !filepaths_in_db.contains(&f) {
            result.insert(f);
        }
    }
    Ok(result)
}

fn process_filelist(
    db: &mut Database,
    filelist: HashSet<PathBuf>,
    commit_batchsize: usize,
) -> Result<()> {
    fn _create_filedigest(path: &PathBuf) -> Result<FileDigest> {
        let digest = get_hash::<Blake2b>(&path)?;
        let s = fs::metadata(&path)?.len();
        Ok(FileDigest {
            id: -1,
            path: path.to_path_buf(),
            digest: digest,
            size: s,
        })
    }

    let (tx, rx) = mpsc::channel();
    rayon::spawn(move || {
        filelist
            .par_iter()
            .map(|path| _create_filedigest(path))
            .try_for_each_with(tx, |tx, f| tx.send(f))
            .expect("expected no send errors");
    });

    let mut filedigests: Vec<FileDigest> = Vec::new();
    let mut time_last_commit = Instant::now();
    for digest in rx.iter() {
        match digest {
            Ok(fd) => filedigests.push(fd),
            Err(err) => log::warn!("Error while processing filelist: {:?}", err),
        };
        if filedigests.len() < commit_batchsize {
            continue;
        }

        // Submitting batch
        let dt = time_last_commit.elapsed().as_secs_f64();
        time_last_commit = Instant::now();
        let total_size_mb = filedigests.iter().map(|f| f.size).sum::<u64>() / (1024 * 1024);
        let mps = total_size_mb as f64 / dt;
        let fps = commit_batchsize as f64 / dt;
        log::debug!(
            "Committing to DB (batch speed: {:3.2} MiB/s, {:3.2} files/s)",
            mps,
            fps
        );
        db.insert_many_filedigests(&filedigests)?;
        filedigests.clear();
    }

    if filedigests.len() > 0 {
        db.insert_many_filedigests(&filedigests)?;
    }
    Ok(())
}

fn update_database<P: AsRef<Path>>(
    db: &mut Database,
    path: P,
    commit_batchsize: usize,
    clean_unfound: bool,
) -> Result<()> {
    log::info!("creating file list");
    let complete_filelist = list_files_in_directory(path);
    log::info!("Number of found files: {:?}", complete_filelist.len());

    if clean_unfound {
        log::info!("Removing outdated files");
        remove_outdated_files(&db, &complete_filelist)?;
    }
    let filelist = filter_out_files_already_in_database(&db, complete_filelist)?;
    log::info!("Number of not already indexed files: {:?}", filelist.len());
    log::info!("hashing");
    process_filelist(db, filelist, commit_batchsize)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = ProgramArguments::from_args();

    let _verbosity = match args.verbose {
        0 => "warn",
        1 => "info",
        _ => "debug",
    };

    if args.verbose < 2 {
        log::warn!("Verbosity is fixed at 'debug' during development");
    }

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "debug"),
    );
    //env_logger::init();
    //log::set_max_level(log::LevelFilter::Debug);

    // We can only call this function once, so here is a sensible place.
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()?;

    log::debug!("cmd args: {:?}", args);

    let mut db = Database::new("./digests.sqlite", args.reset_database)?;

    if !args.path.as_os_str().is_empty() {
        update_database(
            &mut db,
            &args.path,
            args.commit_batchsize,
            args.clean_unfound,
        )?;
    }

    if !args.no_web {
        interface::start_web_interface(db, args.bind_address, args.port, args.allow_preview);
    } else {
        let results = similarities::get_list_of_similar_files(&db)?;
        interface::show_results_in_console(&results);
    }
    log::debug!("exiting");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::prelude::*;
    use tempfile::tempdir;

    #[test]
    fn test_filter_out_files_already_in_database() -> Result<()> {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest {
            id: 1,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 2,
            path: PathBuf::from("/tmp/b"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 3,
            path: PathBuf::from("/tmp/c"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });

        let db = Database::new("test.sqlite", true)?;
        for f in testfiles.iter() {
            db.insert_filedigest(&f)?;
        }

        testfiles.push(FileDigest {
            id: 4,
            path: PathBuf::from("/tmp/d"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 5,
            path: PathBuf::from("/tmp/e"),
            digest: vec![1, 1, 2, 6],
            size: 1,
        });

        let all_files: HashSet<_> = testfiles.iter().map(|f| f.path.clone()).collect();
        let new_files = filter_out_files_already_in_database(&db, all_files)?;
        let target_files: HashSet<_> = testfiles[3..].iter().map(|f| f.path.clone()).collect();
        assert_eq!(new_files, target_files);
        Ok(())
    }

    #[test]
    fn test_remove_outdated_files() -> Result<()> {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest {
            id: 1,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 2,
            path: PathBuf::from("/tmp/b"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 3,
            path: PathBuf::from("/tmp/c"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 4,
            path: PathBuf::from("/tmp/d"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 5,
            path: PathBuf::from("/tmp/e"),
            digest: vec![1, 1, 2, 6],
            size: 1,
        });

        let db = Database::new("test.sqlite", true)?;
        for f in testfiles.iter() {
            db.insert_filedigest(&f)?;
        }

        testfiles.remove(3);
        let remaining_files: HashSet<_> = testfiles.iter().map(|f| f.path.clone()).collect();
        remove_outdated_files(&db, &remaining_files)?;

        let new_files = db.get_all_filedigests()?;
        assert_eq!(new_files, testfiles);
        Ok(())
    }

    #[test]
    fn test_list_files_in_directory() -> Result<()> {
        let dir = PathBuf::from(tempdir()?.path());

        let filelist: HashSet<_> = [
            dir.join("a.txt"),
            dir.join("b"),
            dir.join("subdir1/subdir2/c.md"),
        ]
        .iter()
        .cloned()
        .collect();

        fs::create_dir_all(dir.join("subdir1/subdir2"))?;
        for path in &filelist {
            File::create(path).expect("Failed to create temporary file");
        }

        let all_files = list_files_in_directory(&dir);
        assert_eq!(filelist, all_files);
        Ok(())
    }

    #[test]
    fn test_blake2_hash() -> Result<()> {
        let target_digest = vec![
            162, 118, 77, 19, 58, 22, 129, 107, 88, 71, 167, 55, 167, 134, 242, 236, 228, 193, 72,
            9, 92, 95, 170, 115, 226, 75, 76, 197, 214, 102, 195, 228, 94, 194, 113, 80, 78, 20,
            220, 97, 39, 221, 252, 228, 225, 68, 251, 35, 185, 26, 111, 123, 4, 181, 61, 105, 85,
            2, 41, 7, 34, 149, 59, 15,
        ];

        let tempdir = tempdir()?;
        let filepath = PathBuf::from(tempdir.path()).join("test.txt");
        //println!("{:?}", filepath);
        let mut file = File::create(&filepath)?;
        file.write_all(b"Hello, world!")?;

        let digest = get_hash::<Blake2b>(&filepath)?;
        assert_eq!(digest, target_digest);
        Ok(())
    }

    #[test]
    fn test_process_filelist_and_check_hash() -> Result<()> {
        let target_digest = vec![
            162, 118, 77, 19, 58, 22, 129, 107, 88, 71, 167, 55, 167, 134, 242, 236, 228, 193, 72,
            9, 92, 95, 170, 115, 226, 75, 76, 197, 214, 102, 195, 228, 94, 194, 113, 80, 78, 20,
            220, 97, 39, 221, 252, 228, 225, 68, 251, 35, 185, 26, 111, 123, 4, 181, 61, 105, 85,
            2, 41, 7, 34, 149, 59, 15,
        ];

        let tempdir = tempdir()?;
        let filepath = PathBuf::from(tempdir.path()).join("test.txt");
        println!("{:?}", filepath);
        let mut file = File::create(&filepath)?;
        file.write_all(b"Hello, world!")?;

        let digest = get_hash::<Blake2b>(&filepath)?;
        assert_eq!(digest, target_digest);

        let filelist: HashSet<_> = vec![filepath.clone()].into_iter().collect();
        let mut db = Database::new("test3.sqlite", true)?;
        process_filelist(&mut db, filelist, 16)?;

        let inserted_files = db.get_all_filedigests()?;
        assert_eq!(inserted_files[0].digest, target_digest);
        Ok(())
    }

    #[test]
    fn test_insert_files_multithreaded() -> Result<()> {
        let dir = PathBuf::from(tempdir()?.path());
        let first_path = dir.join("first.txt");
        fs::create_dir_all(&dir)?;
        File::create(first_path.clone()).expect("Failed to create temporary file");

        let mut db = Database::new("test4.sqlite", true)?;

        let mut filelist: HashSet<_> = [
            dir.join("a.txt"),
            dir.join("b"),
            dir.join("subdir1/subdir2/c.md"),
        ]
        .iter()
        .cloned()
        .collect();

        fs::create_dir_all(dir.join("subdir1/subdir2"))?;
        for path in filelist.iter() {
            File::create(path).expect("Failed to create temporary file");
        }
        filelist.insert(first_path);

        process_filelist(&mut db, filelist.clone(), 16)?;

        let all_files = db.get_all_filedigests()?;
        let all_inserted_files: HashSet<_> = all_files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(filelist, all_inserted_files);
        Ok(())
    }
}
