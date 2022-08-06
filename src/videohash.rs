use crate::database::Database;
use anyhow::{anyhow, Result};
use ffmpeg_next as ffmpeg;
use log;
use ndarray::prelude::*;
use rayon::prelude::*;
use rusqlite::params;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{mpsc, Mutex};
use std::time::Instant;

const NUM_BUCKETS_SHIFT: usize = 6;
const NUM_BUCKETS: usize = 256 >> NUM_BUCKETS_SHIFT;

#[derive(Debug, PartialEq, Serialize)]
pub struct VideoHash {
    pub id: i64,
    pub path: String,
    pub histogram: Vec<u8>,
    pub size: u64, // We need size only for logging purposes
}

impl Database {
    fn get_files_without_videohash(&self) -> Result<Vec<(i64, String, u64)>> {
        let mut stmt = self.db.prepare(
            "SELECT id, path, size, lower(substr(path, -3)) as ext FROM file_digests \
             WHERE id NOT IN (SELECT id FROM video_hash) \
             AND ext IN ('mp4', 'avi', 'mkv', 'wmv', 'avi', 'flv')",
        )?;
        let ids: Result<Vec<_>, _> = stmt
            .query_map([], |row| {
                let path_string: String = row.get(1)?;
                Ok((row.get(0)?, path_string, row.get(2)?))
            })?
            .into_iter()
            .collect();
        Ok(ids?)
    }

    fn insert_many_videohashes(&mut self, hashes: &Vec<VideoHash>) -> Result<()> {
        let tx = self.db.transaction()?;
        let mut stmt =
            tx.prepare("INSERT OR IGNORE INTO video_hash (id, histogram) VALUES (?1, ?2)")?;
        for h in hashes {
            let cnt = stmt.execute(params![h.id, h.histogram])?;
            if cnt == 0 {
                return Err(anyhow!("Unable to insert {}", h.id));
            }
        }
        stmt.finalize()?;
        Ok(tx.commit()?)
    }

    pub fn get_all_files_with_videohash(&self) -> Result<Vec<VideoHash>> {
        let mut stmt = self.db.prepare(
            "SELECT f.id, f.path, f.size, h.histogram \
             FROM file_digests f, video_hash h \
             WHERE f.id == h.id",
        )?;
        let files: Result<Vec<_>, _> = stmt
            .query_map([], |row| {
                let path_string: String = row.get(1)?;
                Ok(VideoHash {
                    id: row.get(0)?,
                    path: path_string,
                    size: row.get(2)?,
                    histogram: row.get(3)?,
                })
            })?
            .into_iter()
            .collect();
        Ok(files?)
    }
}

struct Video {
    decoder: ffmpeg::decoder::Video,
    ictx: ffmpeg::format::context::Input,
    scaler: ffmpeg::software::scaling::Context,
    video_stream_index: usize,
}

impl Video {
    fn new(path: impl Into<std::path::PathBuf>, width: u32, height: u32) -> Result<Video> {
        let filepath = path.into();
        log::debug!("Opening {:?}", &filepath);
        // wrapped into immediately invoked function expression so we can catch all errors
        || -> Result<Video> {
            ffmpeg::init()?;
            let ictx = ffmpeg::format::input(&filepath)?;

            let input = ictx
                .streams()
                .best(ffmpeg::media::Type::Video)
                .ok_or(anyhow!("No video stream found"))?;
            let video_stream_index = input.index();

            let decoder = input.codec().decoder().video()?;
            let w = decoder.width();
            let h = decoder.height();

            let scaler = ffmpeg::software::scaling::context::Context::get(
                decoder.format(),
                w,
                h,
                ffmpeg::format::Pixel::RGB24,
                width,
                height,
                ffmpeg::software::scaling::flag::Flags::FAST_BILINEAR,
            )?;

            Ok(Video {
                decoder,
                ictx,
                scaler,
                video_stream_index,
            })
        }()
        .map_err(|e| anyhow!("Unable to open {}: {}", filepath.to_string_lossy(), e))
    }

    fn _decode_frame(&mut self, packet: &ffmpeg::codec::packet::Packet) -> Result<Vec<u8>> {
        let mut decoded = ffmpeg::util::frame::video::Video::empty();
        let mut rgb_frame = ffmpeg::util::frame::video::Video::empty();
        self.decoder.send_packet(packet)?;
        self.decoder.receive_frame(&mut decoded)?;
        self.scaler.run(&decoded, &mut rgb_frame)?;
        return Ok(rgb_frame.data(0).to_vec());
    }
}

impl Iterator for Video {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        loop {
            let next_packet = self.ictx.packets().next();
            if next_packet.is_none() {
                return None;
            }

            let (stream, packet) = next_packet.unwrap();
            if stream.index() != self.video_stream_index {
                continue;
            }
            let frame = self._decode_frame(&packet);
            if frame.is_ok() {
                return Some(frame.unwrap());
            }
        }
    }
}

fn calculate_color_histogram(path: impl Into<std::path::PathBuf> + Clone) -> Result<Vec<u8>> {
    const VIDEO_WIDTH: u32 = 128;
    const VIDEO_HEIGHT: u32 = 128;
    let mut histogram = Array::<u64, _>::zeros((NUM_BUCKETS, NUM_BUCKETS, NUM_BUCKETS));
    let video = Video::new(path, VIDEO_HEIGHT, VIDEO_WIDTH)?;
    let mut num_pixel: u64 = 0;
    let pixel_per_frame: usize = (VIDEO_HEIGHT * VIDEO_WIDTH) as usize;
    for v in video {
        for i in 0..pixel_per_frame {
            let idx = i * 3;
            let r: usize = (v[idx + 0] >> NUM_BUCKETS_SHIFT).into();
            let g: usize = (v[idx + 1] >> NUM_BUCKETS_SHIFT).into();
            let b: usize = (v[idx + 2] >> NUM_BUCKETS_SHIFT).into();
            histogram[[r, g, b]] += 1;
            num_pixel += 1;
        }
    }

    // We bin the counts into different bins
    let n = num_pixel as f64;
    let max = u8::MAX as f64;
    let binned_histogram = histogram.map(|x| ((max * (*x) as f64) / n) as u8);
    let num_elements = binned_histogram.len();
    let flat_histogram = binned_histogram.into_shape(num_elements)?;
    Ok(flat_histogram.to_vec())
}

fn _create_hash(
    id: i64,
    path: impl Into<std::path::PathBuf> + Clone,
    size: u64,
) -> Result<VideoHash> {
    let h = calculate_color_histogram(path)?;
    Ok(VideoHash {
        id: id,
        histogram: h,
        size: size,
        path: String::new(),
    })
}

fn get_files_without_videohash(db_mutex: &Mutex<Database>) -> Result<Vec<(i64, String, u64)>> {
    if let Ok(db) = db_mutex.lock() {
        return Ok(db.get_files_without_videohash()?);
    } else {
        return Err(anyhow!("Unable to lock DB"));
    }
}

pub fn update_hashes(db_mutex: &Mutex<Database>, commit_batchsize: usize) -> Result<()> {
    let filelist = get_files_without_videohash(db_mutex)?;
    log::info!("Files to process: {:?}", filelist.len());
    let (tx, rx) = mpsc::channel();
    rayon::spawn(move || {
        filelist
            .par_iter()
            .map(|x| _create_hash(x.0, &x.1, x.2))
            .try_for_each_with(tx, |tx, f| tx.send(f))
            .expect("expected no send errors");
    });

    let mut hashes: Vec<VideoHash> = Vec::new();
    let mut time_last_commit = Instant::now();
    for hist in rx.iter() {
        match hist {
            Ok(h) => hashes.push(h),
            Err(err) => log::warn!("Error while processing filelist: {:?}", err),
        };
        if hashes.len() < commit_batchsize {
            continue;
        }

        // Submitting batch
        let dt = time_last_commit.elapsed().as_secs_f64();
        time_last_commit = Instant::now();
        let total_size_mb = hashes.iter().map(|f| f.size).sum::<u64>() / (1024 * 1024);
        let mps = total_size_mb as f64 / dt;
        let fps = commit_batchsize as f64 / dt;
        log::debug!(
            "Committing to DB (speed: {:3.2} MiB/s, {:3.2} files/s)",
            mps,
            fps
        );
        if let Ok(mut db) = db_mutex.lock() {
            db.insert_many_videohashes(&hashes)?;
        } else {
            return Err(anyhow!("Unable to lock DB"));
        }
        hashes.clear();
    }

    if hashes.len() > 0 {
        if let Ok(mut db) = db_mutex.lock() {
            db.insert_many_videohashes(&hashes)?;
        } else {
            return Err(anyhow!("Unable to lock DB"));
        }
    }
    Ok(())
}

fn l1_distance(a: &Vec<u8>, b: &Vec<u8>) -> u16 {
    let mut dist = 0;
    for i in 0..a.len() {
        dist += (a[i] as i16 - b[i] as i16).abs();
    }
    dist as u16
}

pub fn calculate_distances(files: &Vec<VideoHash>) -> Array2<u16> {
    let mut dist: Array2<u16> = Array::zeros((files.len(), files.len()));
    for (i, a) in files.iter().enumerate() {
        for j in i..files.len() {
            let b = &files[j];
            dist[[i, j]] = if i != j {
                l1_distance(&a.histogram, &b.histogram)
            } else {
                0
            };
            dist[[j, i]] = dist[[i, j]];
        }
    }
    dist
}

pub fn find_similar_files<'a, 'b>(
    files: &'a Vec<VideoHash>,
    dist: &'b Array2<u16>,
    threshold: u16,
) -> Vec<Vec<&'a VideoHash>> {
    // datastructures and functions for Union-Find
    let mut parent = Vec::with_capacity(files.len());
    fn _find(y: usize, parent: &mut Vec<usize>) -> usize {
        let mut x = y;
        while parent[x] != x {
            let tmp = x;
            x = parent[x];
            parent[tmp] = parent[parent[x]];
        }
        return x;
    }
    fn _union(x: usize, y: usize, parent: &mut Vec<usize>) {
        let x_root = _find(x, parent);
        let y_root = _find(y, parent);

        if x_root == y_root {
            return;
        }

        // TODO: no union by size/rank
        parent[x_root] = y_root;
    }

    // files[i] is stored at parent[i]
    for i in 0..files.len() {
        parent.push(i);
    }
    for i in 0..files.len() {
        if files[i].histogram.iter().all(|&x| x == 0) {
            continue;
        }
        for j in i..files.len() {
            if dist[[i, j]] < threshold {
                _union(i, j, &mut parent);
            }
        }
    }

    let mut filebags = HashMap::new();
    for (idx, f) in files.iter().enumerate() {
        let parent_idx = _find(idx, &mut parent);
        let bag = filebags
            .entry(parent_idx)
            .or_insert(Vec::<&VideoHash>::new());
        bag.push(f);
    }

    filebags.into_values().filter(|x| x.len() > 1).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // only used during development
    //#[test]
    fn _test_color_() -> Result<()> {
        let h = calculate_color_histogram("/media/scratch/vid1_720p.mp4")?;
        //println!("Histogram shape: {:?}, sum: {}", h.shape(), h.sum());
        println!("Histogram: {:?}", h);
        Ok(())
    }

    #[test]
    fn test_get_files_without_videohash() -> Result<()> {
        let db = Database::new("test_get_files_without_videohash.sqlite", true)?;
        db.db.execute(
            "INSERT INTO file_digests (id, path, size) VALUES \
                (1, '/tmp/a.mp4', 1), (2, '/tmp/b.jpg', 1), 
                (3, '/tmp/c.wmv', 1), (4, '/tmp/d.avi', 1)",
            params![],
        )?;

        db.db.execute(
            "INSERT INTO video_hash (id, histogram) VALUES (3, 0)",
            params![],
        )?;

        let files = db.get_files_without_videohash()?;
        let ids: Vec<i64> = files.into_iter().map(|x| x.0).collect();
        assert_eq!(ids, [1, 4]);
        Ok(())
    }

    #[test]
    fn test_get_all_files_with_videohash() -> Result<()> {
        let db = Database::new("test_get_all_files_with_videohash.sqlite", true)?;
        db.db.execute(
            "INSERT INTO file_digests (id, path, size) VALUES \
                (1, '/tmp/a.mp4', 10), (2, '/tmp/b.jpg', 11), 
                (3, '/tmp/c.wmv', 12), (4, '/tmp/d.avi', 13)",
            params![],
        )?;

        db.db.execute(
            "INSERT INTO video_hash (id, histogram) VALUES \
            (3, x'aaaaaaaa'), (4, x'aaaaaaab')",
            params![],
        )?;

        let files = db.get_all_files_with_videohash()?;

        // TODO: this test relies on the order of the returned files
        let mut target_list = Vec::new();
        target_list.push(VideoHash {
            id: 3,
            path: "/tmp/c.wmv".to_string(),
            size: 12,
            histogram: vec![170, 170, 170, 170],
        });
        target_list.push(VideoHash {
            id: 4,
            path: "/tmp/d.avi".to_string(),
            size: 13,
            histogram: vec![170, 170, 170, 171],
        });
        assert_eq!(files, target_list);
        Ok(())
    }

    #[test]
    fn test_find_similar_files() -> Result<()> {
        let db = Database::new("test_find_similar_files.sqlite", true)?;
        db.db.execute(
            "INSERT INTO file_digests (id, path, size) VALUES \
                (1, '/tmp/a.mp4', 10), (2, '/tmp/b.mp4', 11), 
                (3, '/tmp/c.wmv', 12), (4, '/tmp/d.avi', 13),
                (5, 'tmp/e.wmv', 15)",
            params![],
        )?;

        db.db.execute(
            "INSERT INTO video_hash (id, histogram) VALUES \
            (1, x'ff00ff00'), (2, x'ff01ff00'), (3, x'000000a0'), \
            (4, x'00ff00ff'), (5, x'000000a2') ",
            params![],
        )?;
        let files = db.get_all_files_with_videohash()?;
        let threshold = 128;
        let dist = calculate_distances(&files);
        let similar_files = find_similar_files(&files, &dist, threshold);
        let res: HashSet<Vec<i64>> = similar_files
            .iter()
            .map(|b| b.iter().map(|x| x.id).collect())
            .collect();
        let expected = HashSet::from([vec![3, 5], vec![1, 2]]);
        assert_eq!(res, expected);
        Ok(())
    }
}
