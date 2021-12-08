use anyhow::Result;
use glob::glob;
use log;
use std::collections::HashSet;

use std::path::{Path, PathBuf};

use structopt::StructOpt;

mod database;
pub use crate::database::{Database, FileDigest};

mod interface;
pub use crate::interface::*;

mod similarities;
pub use crate::similarities::*;

mod filehashing;
pub use crate::filehashing::*;

mod videohash;
pub use crate::videohash::*;

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

    /// Enable similarity-search via color histograms
    #[structopt(long)]
    videohash: bool,
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

fn update_database<P: AsRef<Path>>(
    db: &mut Database,
    path: P,
    commit_batchsize: usize,
    clean_unfound: bool,
    update_videohash: bool,
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
    filehashing::process_filelist(db, filelist, commit_batchsize)?;
    if update_videohash {
        log::info!("Creating video hashes");
        videohash::update_hashes(db, commit_batchsize)?;
    }
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
            args.videohash,
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
    use rusqlite::params;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_filter_out_files_already_in_database() -> Result<()> {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest::new(1, "/tmp/a", vec![0, 1, 2, 3], 1));
        testfiles.push(FileDigest::new(2, "/tmp/b", vec![0, 1, 2, 3], 1));
        testfiles.push(FileDigest::new(3, "/tmp/c", vec![0, 1, 2, 4], 1));

        let db = Database::new("test.sqlite", true)?;
        for f in testfiles.iter() {
            db.insert_filedigest(&f)?;
        }

        testfiles.push(FileDigest::new(4, "/tmp/d", vec![0, 1, 2, 4], 1));
        testfiles.push(FileDigest::new(5, "/tmp/e", vec![0, 1, 2, 5], 1));

        let all_files: HashSet<_> = testfiles.iter().map(|f| f.path.clone()).collect();
        let new_files = filter_out_files_already_in_database(&db, all_files)?;
        let target_files: HashSet<_> = testfiles[3..].iter().map(|f| f.path.clone()).collect();
        assert_eq!(new_files, target_files);
        Ok(())
    }

    #[test]
    fn test_remove_outdated_files() -> Result<()> {
        let db = Database::new("test.sqlite", true)?;
        db.db.execute(
            "INSERT INTO file_digests (id, path, digest, size) VALUES \
                (1, '/tmp/a', x'aaaaaaaa', 2), 
                (2, '/tmp/b', x'aaaaaaaa', 2), 
                (3, '/tmp/c', x'aaaaaaab', 1), 
                (4, '/tmp/d', x'aaaaaaab', 3), 
                (5, '/tmp/e', x'aaaaaaac', 1)",
            params![],
        )?;
        let mut testfiles = db.get_all_filedigests()?;

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
            fs::File::create(path).expect("Failed to create temporary file");
        }

        let all_files = list_files_in_directory(&dir);
        assert_eq!(filelist, all_files);
        Ok(())
    }
}
