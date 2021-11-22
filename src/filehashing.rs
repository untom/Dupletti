use anyhow::Result;
use blake2::{Blake2b, Digest};
use rayon::prelude::*;
use rusqlite::params;
use simple_error::SimpleError;
use std::fs;
use std::io::{self, Read};
use std::sync::mpsc;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::database::{Database, FileDigest};

impl Database {
    fn insert_many_filedigests(&mut self, files: &Vec<FileDigest>) -> Result<()> {
        let tx = self.db.transaction()?;
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO file_digests (path, digest, size) VALUES (?1, ?2, ?3)",
        )?;
        for f in files {
            // TODO: raise Error when _cnt == 0, because that means we re-inserted a path.
            let path = f.path.to_string_lossy();
            let cnt = stmt.execute(params![path, f.digest, f.size])?;
            if cnt == 0 {
                let err = SimpleError::new(format!("Unable to insert {}", path));
                return Err(anyhow::Error::new(err));
            }
        }
        stmt.finalize()?;
        Ok(tx.commit()?)
    }
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

pub fn process_filelist(
    db: &mut Database,
    filelist: HashSet<PathBuf>,
    commit_batchsize: usize,
) -> Result<()> {
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
            "Committing to DB (speed: {:3.2} MiB/s, {:3.2} files/s)",
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::prelude::*;
    use tempfile::tempdir;

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

    #[test]
    fn test_insert_many_filedigests() -> Result<()> {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest::new(1, "/tmp/a", vec![0, 1, 2, 3], 1));
        testfiles.push(FileDigest::new(2, "/tmp/b", vec![0, 1, 2, 3], 1));
        testfiles.push(FileDigest::new(3, "/tmp/c", vec![0, 1, 2, 4], 1));
        testfiles.push(FileDigest::new(4, "/tmp/d", vec![0, 1, 2, 4], 1));
        testfiles.push(FileDigest::new(5, "/tmp/e", vec![0, 1, 2, 5], 1));

        let mut db = Database::new("test6.sqlite", true)?;
        db.insert_many_filedigests(&testfiles)?;
        let result = db.get_all_filedigests()?;
        assert_eq!(testfiles, result);
        Ok(())
    }
}
