use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::path::PathBuf;

pub use crate::database::{Database, FileDigest};

#[derive(Debug, PartialEq, Serialize)]
pub struct FileEntry {
    pub id: i64,
    pub path: PathBuf,
    pub size: u64,
}

#[derive(Debug)]
struct FileDigestBag {
    id_list: Vec<i64>,
    digest: Vec<u8>,
}

fn find_similarities(files: Vec<FileDigest>) -> HashSet<Vec<i64>> {
    let mut map = HashMap::new();
    for file in files {
        // Looking up 4bytes instead of 1byte reduces this function's time
        // for 30k files from >1m to <1s
        // file-digests are >4 bytes, so the unwrap should always work
        let lookup_value: [u8; 4] = file.digest[0..4].try_into().unwrap();
        let candidate_bags = map
            .entry(lookup_value)
            .or_insert(Vec::<FileDigestBag>::new());
        let mut is_inserted = false;
        for bag in candidate_bags.iter_mut() {
            if file.digest == bag.digest {
                bag.id_list.push(file.id);
                is_inserted = true;
            }
        }
        if !is_inserted {
            candidate_bags.push(FileDigestBag {
                id_list: vec![file.id],
                digest: file.digest,
            })
        }
    }
    let mut result = HashSet::new();
    for (_, candidate_bags) in map {
        for mut bag in candidate_bags {
            if bag.id_list.len() > 1 {
                bag.id_list.sort_unstable(); // guarantee order of result
                result.insert(bag.id_list);
            }
        }
    }
    result
}

fn into_resultbag(db: &Database, similar_files: &HashSet<Vec<i64>>) -> Result<Vec<Vec<FileEntry>>> {
    let mut bags = Vec::new();
    for id_list in similar_files {
        let files: Vec<FileEntry> = id_list
            .iter()
            .map(|id| {
                let f = db.lookup_filedigest(*id)?;
                Ok(FileEntry {
                    id: f.id,
                    path: f.path,
                    size: f.size,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        bags.push(files);
    }

    bags.sort_unstable_by_key(|k| -(k[0].size as i64));
    Ok(bags)
}

pub fn get_list_of_similar_files(db: &Database) -> Result<Vec<Vec<FileEntry>>> {
    let files = db.get_all_filedigests()?;
    log::info!("looking for similarities between {} files", files.len());
    let similar_files = find_similarities(files);
    log::info!("creating result bags");
    let results = into_resultbag(&db, &similar_files)?;
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_resultbag() -> Result<()> {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest {
            id: 1,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 2,
        });
        testfiles.push(FileDigest {
            id: 2,
            path: PathBuf::from("/tmp/b"),
            digest: vec![0, 1, 2, 3],
            size: 2,
        });
        testfiles.push(FileDigest {
            id: 3,
            path: PathBuf::from("/tmp/d"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 4,
            path: PathBuf::from("/tmp/e"),
            digest: vec![1, 1, 2, 5],
            size: 3,
        });
        testfiles.push(FileDigest {
            id: 5,
            path: PathBuf::from("/tmp/c"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 6,
            path: PathBuf::from("/tmp/f"),
            digest: vec![1, 1, 2, 5],
            size: 3,
        });
        testfiles.push(FileDigest {
            id: 7,
            path: PathBuf::from("/tmp/g"),
            digest: vec![1, 1, 2, 7],
            size: 4,
        });

        let db = Database::new("test.sqlite", true)?;
        for f in &testfiles {
            db.insert_filedigest(f)?;
        }

        let similar_files = find_similarities(testfiles);
        let results = into_resultbag(&db, &similar_files)?;

        // TODO: this relies on the DB to retrieve filedigests in the order they were inserted
        let target = vec![
            vec![
                FileEntry {
                    id: 4,
                    path: PathBuf::from("/tmp/e"),
                    size: 3,
                },
                FileEntry {
                    id: 6,
                    path: PathBuf::from("/tmp/f"),
                    size: 3,
                },
            ],
            vec![
                FileEntry {
                    id: 1,
                    path: PathBuf::from("/tmp/a"),
                    size: 2,
                },
                FileEntry {
                    id: 2,
                    path: PathBuf::from("/tmp/b"),
                    size: 2,
                },
            ],
            vec![
                FileEntry {
                    id: 3,
                    path: PathBuf::from("/tmp/d"),
                    size: 1,
                },
                FileEntry {
                    id: 5,
                    path: PathBuf::from("/tmp/c"),
                    size: 1,
                },
            ],
        ];
        assert_eq!(results, target);
        Ok(())
    }

    #[test]
    fn test_find_similarities() {
        let mut testfiles = Vec::new();
        testfiles.push(FileDigest {
            id: 1,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 2,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 3,
            path: PathBuf::from("/tmp/a"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 4,
            path: PathBuf::from("/tmp/a"),
            digest: vec![1, 1, 2, 4],
            size: 1,
        });
        testfiles.push(FileDigest {
            id: 5,
            path: PathBuf::from("/tmp/a"),
            digest: vec![1, 1, 2, 6],
            size: 1,
        });

        let list_of_similar_files = find_similarities(testfiles);

        let mut target_sim_list = HashSet::new();
        target_sim_list.insert(vec![1, 2]);
        target_sim_list.insert(vec![3, 4]);
        assert_eq!(list_of_similar_files, target_sim_list);
    }

    use rand::Rng;
    use std::time::Instant;

    //#[test]
    #[allow(dead_code)]
    fn unused_benchmark_find_similarities() {
        let mut files = Vec::new();
        let mut rng = rand::thread_rng();

        let num_sampes = 5_000_000;
        let digest_len = 4; //1024;
        for i in 0..num_sampes {
            // Use a very small range for the digest, so we get lots of collisions
            //let digest: Vec<u8> = (0..digest_len).map(|_| rng.gen::<u8>()).collect();
            let digest: Vec<u8> = (0..digest_len).map(|_| rng.gen_range(0..5)).collect();
            let path = PathBuf::from(format!("/tmp/a{}", i));
            files.push(FileDigest {
                id: i,
                path: path,
                digest: digest,
                size: 42,
            });
        }
        let t0 = Instant::now();
        let _list_of_similar_files = find_similarities(files);
        let dt = t0.elapsed().as_secs_f32();
        println!("Elapsed Time: {}", dt);
    }
}
