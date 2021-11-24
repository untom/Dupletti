use crate::database::Database;
use anyhow::{anyhow, Result};
use ffmpeg_next as ffmpeg;
use log;
use ndarray::prelude::*;
use rayon::prelude::*;
use rusqlite::params;
use simple_error::SimpleError;
use std::{convert::TryFrom, sync::mpsc, time::Instant};

pub struct VideoHistogram {
    pub id: i64,
    pub histogram: Vec<u8>,
    pub size: u64, // We need size only for logging purposes
}

impl Database {
    fn get_files_without_histogram(&self) -> Result<Vec<(i64, String, u64)>> {
        let mut stmt = self.db.prepare(
            "SELECT id, path, size, lower(substr(path, -3)) as ext FROM file_digests \
                WHERE id NOT IN (SELECT id FROM video_histograms) \
                      AND ext IN ('mp4', 'avi', 'mkv', 'wmv', 'avi')",
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

    fn insert_many_histograms(&mut self, histograms: &Vec<VideoHistogram>) -> Result<()> {
        let tx = self.db.transaction()?;
        let mut stmt =
            tx.prepare("INSERT OR IGNORE INTO video_histograms (id, histogram) VALUES (?1, ?2)")?;
        for h in histograms {
            let cnt = stmt.execute(params![h.id, h.histogram])?;
            if cnt == 0 {
                let err = SimpleError::new(format!("Unable to insert {}", h.id));
                return Err(anyhow::Error::new(err));
            }
        }
        stmt.finalize()?;
        Ok(tx.commit()?)
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
        ffmpeg::init().unwrap();
        let path_into = path.into();

        let ictx = ffmpeg::format::input(&path_into)?;
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

        // log::debug!("Opened {:?}: {}x{}", &path_into, w, h);
        Ok(Video {
            decoder,
            ictx,
            scaler,
            video_stream_index,
        })
    }
}

impl Iterator for Video {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        loop {
            let next_packet = self.ictx.packets().next();
            if next_packet.is_some() {
                let (stream, packet) = next_packet.unwrap();
                if stream.index() == self.video_stream_index {
                    match self.decoder.send_packet(&packet) {
                        Ok(_) => {
                            let mut decoded = ffmpeg::util::frame::video::Video::empty();
                            if self.decoder.receive_frame(&mut decoded).is_ok() {
                                let mut rgb_frame = ffmpeg::util::frame::video::Video::empty();
                                self.scaler.run(&decoded, &mut rgb_frame).unwrap();
                                return Some(rgb_frame.data(0).to_vec());
                            }
                        }
                        Err(_) => (), // some packages contain mistakes, no big deal usually
                    }
                }
            } else {
                return None;
            }
        }
    }
}

fn calculate_histogram(path: impl Into<std::path::PathBuf>) -> Result<Vec<u8>> {
    const VIDEO_WIDTH: u32 = 128;
    const VIDEO_HEIGHT: u32 = 128;
    const NUM_BUCKETS_SHIFT: i32 = 6;
    const NUM_BUCKETS: usize = 256 >> NUM_BUCKETS_SHIFT;
    let mut histogram = Array::<u64, _>::zeros((NUM_BUCKETS, NUM_BUCKETS, NUM_BUCKETS));
    let video = Video::new(path, VIDEO_HEIGHT, VIDEO_WIDTH)?;
    let mut num_pixel = 0;
    for v in video {
        for i in 0..VIDEO_HEIGHT * VIDEO_WIDTH {
            let idx: usize = usize::try_from(i * 3).unwrap();
            let r: usize = (v[idx + 0] >> NUM_BUCKETS_SHIFT as usize).into();
            let g: usize = (v[idx + 1] >> NUM_BUCKETS_SHIFT as usize).into();
            let b: usize = (v[idx + 2] >> NUM_BUCKETS_SHIFT as usize).into();
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

fn _create_histogram(
    id: i64,
    path: impl Into<std::path::PathBuf>,
    size: u64,
) -> Result<VideoHistogram> {
    let h = calculate_histogram(path)?;
    Ok(VideoHistogram {
        id: id,
        histogram: h,
        size: size,
    })
}

pub fn update_histograms(db: &mut Database, commit_batchsize: usize) -> Result<()> {
    let filelist = db.get_files_without_histogram()?;
    log::info!("Files to process: {:?}", filelist.len());
    let (tx, rx) = mpsc::channel();
    rayon::spawn(move || {
        filelist
            .par_iter()
            .map(|x| _create_histogram(x.0, &x.1, x.2))
            .try_for_each_with(tx, |tx, f| tx.send(f))
            .expect("expected no send errors");
    });

    let mut histograms: Vec<VideoHistogram> = Vec::new();
    let mut time_last_commit = Instant::now();
    for hist in rx.iter() {
        match hist {
            Ok(h) => histograms.push(h),
            Err(err) => log::warn!("Error while processing filelist: {:?}", err),
        };
        if histograms.len() < commit_batchsize {
            continue;
        }

        // Submitting batch
        let dt = time_last_commit.elapsed().as_secs_f64();
        time_last_commit = Instant::now();
        let total_size_mb = histograms.iter().map(|f| f.size).sum::<u64>() / (1024 * 1024);
        let mps = total_size_mb as f64 / dt;
        let fps = commit_batchsize as f64 / dt;
        log::debug!(
            "Committing to DB (speed: {:3.2} MiB/s, {:3.2} files/s)",
            mps,
            fps
        );
        db.insert_many_histograms(&histograms)?;
        histograms.clear();
    }

    if histograms.len() > 0 {
        db.insert_many_histograms(&histograms)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // only used during development
    //#[test]
    fn _test_calculate_histogram() -> Result<()> {
        let h = calculate_histogram("/media/scratch/vid1_360p.mp4")?;
        //println!("Histogram shape: {:?}, sum: {}", h.shape(), h.sum());
        println!("Histogram: {:?}", h);
        Ok(())
    }

    #[test]
    fn test_get_files_without_histogram() -> Result<()> {
        let db = Database::new("test_get_files_without_histogram.sqlite", true)?;
        db.db.execute(
            "INSERT INTO file_digests (id, path, size) VALUES \
                (1, '/tmp/a.mp4', 1), 
                (2, '/tmp/b.jpg', 1), 
                (3, '/tmp/c.wmv', 1), 
                (4, '/tmp/d.avi', 1)",
            params![],
        )?;

        db.db.execute(
            "INSERT INTO video_histograms (id, histogram) VALUES (3, 0)",
            params![],
        )?;

        let files = db.get_files_without_histogram()?;
        let ids: Vec<i64> = files.into_iter().map(|x| x.0).collect();
        assert_eq!(ids, [1, 4]);
        Ok(())
    }
}
