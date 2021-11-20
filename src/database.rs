use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use simple_error::SimpleError;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Clone)]
pub struct FileDigest {
    pub id: i64,
    pub path: PathBuf,
    pub digest: Vec<u8>,
    pub size: u64,
}

impl FileDigest {
    pub fn new(id: i64, path: &str, digest: Vec<u8>, size: u64) -> FileDigest {
        FileDigest {
            id: id,
            path: PathBuf::from(path),
            digest: digest,
            size: size,
        }
    }
}

pub struct Database {
    pub db: Connection,
}

impl Database {
    pub fn new<P: AsRef<Path>>(filepath: P, reset: bool) -> Result<Database> {
        let db = Database {
            db: Connection::open(filepath)?,
        };
        if reset {
            db.db
                .execute("DROP TABLE IF EXISTS file_digests", params![])?;
            db.db
                .execute("DROP TABLE IF EXISTS video_histograms", params![])?;
        }
        db.db
            .execute(
                "CREATE TABLE IF NOT EXISTS file_digests (
					id    	INTEGER PRIMARY KEY,
					path   	TEXT NOT NULL UNIQUE,
					digest	BLOB,
					size  	INTEGER     
					)",
                params![],
            )
            .context("Creating Database")?;
        Ok(db)
    }

    pub fn get_all_filedigests(&self) -> Result<Vec<FileDigest>> {
        let mut stmt = self
            .db
            .prepare("SELECT id, path, digest, size FROM file_digests")?;
        let rows: Result<Vec<_>, _> = stmt
            .query_map([], |row| {
                let path_string: String = row.get(1)?;
                Ok(FileDigest {
                    id: row.get(0)?,
                    path: PathBuf::from(path_string),
                    digest: row.get(2)?,
                    size: row.get(3)?,
                })
            })?
            .into_iter()
            .collect();
        Ok(rows?)
    }

    pub fn insert_filedigest(&self, file: &FileDigest) -> Result<()> {
        // use INSERT OR IGNORE in case we're mistakenly trying to insert something twice
        let path = file.path.to_string_lossy();
        let cnt = self.db.execute(
            "INSERT OR IGNORE INTO file_digests (path, digest, size) VALUES (?1, ?2, ?3)",
            params![path, file.digest, file.size],
        )?;
        if cnt == 0 {
            let err = SimpleError::new(format!("Unable to insert {}", path));
            return Err(anyhow::Error::new(err));
        }
        Ok(())
    }

    pub fn lookup_filedigest(&self, file_id: i64) -> Result<FileDigest> {
        Ok(self.db.query_row(
            "SELECT  id, path, digest, size FROM file_digests WHERE id =(?1)",
            params![file_id],
            |row| {
                let path_string: String = row.get(1)?;
                Ok(FileDigest {
                    id: row.get(0)?,
                    path: PathBuf::from(path_string),
                    digest: row.get(2)?,
                    size: row.get(3)?,
                })
            },
        )?)
    }

    pub fn delete_filedigest(&self, file_id: i64) -> Result<usize> {
        Ok(self
            .db
            .execute("DELETE FROM file_digests WHERE id =(?1)", params![file_id])?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;
    use std::fs;
    use std::io;
    use std::sync::mpsc;

    #[test]
    fn test_insert_file() -> Result<()> {
        let db = Database::new("test1.sqlite", true)?;
        let file = FileDigest::new(1, "/tmp/a", vec![0, 1, 2, 3], 1);
        db.insert_filedigest(&file)?;
        let inserted_files = db.get_all_filedigests()?;
        let target = vec![file];
        assert_eq!(inserted_files, target);
        Ok(())
    }

    #[test]
    fn test_lookup_file_by_index() -> Result<()> {
        let db = Database::new("test2.sqlite", true)?;
        let target_path = "/tmp/abcde";
        let file1 = FileDigest::new(1, "/tmp/abc", vec![0, 1, 2, 3], 1);
        let file2 = FileDigest::new(2, target_path.clone(), vec![0, 1, 2, 3], 1);
        let file3 = FileDigest::new(3, "/tmp/cde", vec![0, 1, 2, 3], 1);
        db.insert_filedigest(&file1)?;
        db.insert_filedigest(&file2)?;
        db.insert_filedigest(&file3)?;

        let file = db.lookup_filedigest(2)?;
        assert_eq!(file, file2);
        Ok(())
    }

    /// This was used to update an existing DB.
    //#[test]
    #[allow(dead_code)]
    fn unused_update_sizes() -> Result<()> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build_global()?;

        let mut db = Database::new("digests.sqlite", false)?;
        //let mut stmt = db.db.prepare("ALTER TABLE file_digests ADD COLUMN size INTEGER")?;
        //stmt.execute(params![])?;
        let mut stmt = db
            .db
            .prepare("SELECT id, path FROM file_digests WHERE size is NULL")?;
        let rows_tmp: Result<Vec<_>, _> = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let path: String = row.get(1)?;
                Ok((id, PathBuf::from(path)))
            })?
            .into_iter()
            .collect();
        let rows = rows_tmp?;
        stmt.finalize()?;

        println!("Number of files: {}", rows.len());

        let (tx, rx) = mpsc::channel();
        rayon::spawn(move || {
            rows.par_iter()
                .map(|(id, path)| -> io::Result<(i64, u64)> {
                    let s = fs::metadata(&path)?.len();
                    Ok((*id, s))
                })
                .try_for_each_with(tx, |tx, id_and_size| tx.send(id_and_size))
                .expect("expected no send errors");
        });

        let tx = db.db.transaction()?;
        let mut update_stmt = tx.prepare("UPDATE file_digests SET size = ?1 WHERE id = ?2")?;

        let mut counter = 0;
        for tmp in rx {
            match tmp {
                Ok((id, s)) => {
                    counter += update_stmt.execute(params![s, id])?;
                }
                Err(err) => {
                    println!("Error while processing filelist: {:?}", err);
                }
            }
        }
        update_stmt.finalize()?;
        tx.commit()?;

        println!("Counter: {}", counter);
        Ok(())
    }

    #[test]
    fn test_insert_file_twice() -> Result<()> {
        let db = Database::new("test4.sqlite", true)?;
        let file1 = FileDigest::new(1, "/tmp/a", vec![0, 1, 2, 3], 1);
        let file2 = FileDigest::new(2, "/tmp/a", vec![0, 1, 2, 4], 1);
        db.insert_filedigest(&file1)?;
        let throws_error = match db.insert_filedigest(&file2) {
            Ok(_) => false,
            Err(_) => true,
        };
        assert!(throws_error);
        Ok(())
    }
}
