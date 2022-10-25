use crate::database::Database;
use crate::similarities;
use crate::videohash;
use anyhow::{anyhow, Result};
use log;
use ndarray::prelude::*;
use rouille::{router, Response};
use rusqlite::params;
use std::fs;
use std::sync::{Arc, Mutex};
use tera::{Context as TeraContext, Tera};

impl Database {
    fn rename_file(&self, file_id: i64, new_path: String) -> Result<()> {
        self.db.execute(
            "UPDATE file_digests SET path = (?1) WHERE id =(?2)",
            params![new_path, file_id],
        )?;
        log::debug!("DB: renaming {} to {}", file_id, new_path);
        Ok(())
    }
}

pub fn show_results_in_console(result: &Vec<Vec<similarities::FileEntry>>) {
    let mut total_size_saved = 0;
    let mut print_nl = false;
    for bag in result {
        for (i, f) in bag.iter().enumerate() {
            if i > 0 {
                total_size_saved += f.size;
            }
            let s = f.size as f64 / (1024. * 1024. * 1024.);
            if s > 1.0 {
                let p = f.path.to_string_lossy();
                println!("{0:>4.2} GB: {1}", s, p);
                print_nl = true;
            }
        }
        if print_nl {
            println!();
            print_nl = false;
        }
    }

    let total_size_gb = total_size_saved as f64 / (1024.0 * 1024.0 * 1024.0);
    println!("Total saved size: {:.2} GB", total_size_gb);
}

pub fn render_results_to_html(
    result: &Vec<Vec<similarities::FileEntry>>,
    tera: &Tera,
    allow_preview: bool,
) -> Result<String> {
    log::debug!("rendering to HTML");
    let mut context = TeraContext::new();
    context.insert("result", result);
    context.insert("allow_preview", &allow_preview);
    let html = tera.render("results.html.tera", &context)?;
    Ok(html)
}

pub fn render_videohash_results_to_html(
    result: Vec<Vec<&videohash::VideoHash>>,
    tera: &Tera,
    allow_preview: bool,
) -> Result<String> {
    log::debug!("rendering to HTML");
    let mut context = TeraContext::new();
    context.insert("result", &result);
    context.insert("allow_preview", &allow_preview);
    let html = tera.render("videohash.html.tera", &context)?;
    Ok(html)
}

fn rename_file(db: &Database, id: i64, new_name: String) -> Result<&str> {
    let file = db.lookup_filedigest(id)?;
    let status = if file.path.exists() {
        fs::rename(file.path, &new_name)?;
        "success"
    } else {
        "does-not-exist"
    };
    db.rename_file(id, new_name)?;
    Ok(status)
}

fn delete_file(db: &Database, id: i64) -> Result<&str> {
    let file = db.lookup_filedigest(id)?;
    let status = if file.path.exists() {
        fs::remove_file(file.path)?;
        "success"
    } else {
        "does-not-exist"
    };
    db.delete_filedigest(id)?;
    Ok(status)
}

fn handle_index_request(
    db_mutex: &Mutex<Database>,
    tera: &Tera,
    allow_preview: bool,
) -> Result<Response> {
    if let Ok(db) = db_mutex.lock() {
        let results = similarities::get_list_of_similar_files(&db)?;
        let html = render_results_to_html(&results, &tera, allow_preview).unwrap();
        Ok(Response::html(html))
    } else {
        return Err(anyhow!("Unable to lock DB"));
    }
}

fn handle_preview_request(db_mutex: &Mutex<Database>, file_id: i64) -> Result<Response> {
    if let Ok(db) = db_mutex.lock() {
        let filepath = db.lookup_filedigest(file_id)?.path;
        let extension = filepath.extension().and_then(|s| s.to_str()).unwrap_or("");
        let file = fs::File::open(&filepath)?;
        Ok(Response::from_file(rouille::extension_to_mime(extension), file).with_no_cache())
    // files might be big, so don't cache them
    } else {
        return Err(anyhow!("Unable to lock DB"));
    }
}

pub struct VideoHashData {
    pub hashes: Vec<videohash::VideoHash>,
    pub distances: Array2<u16>,
}

impl VideoHashData {
    pub fn new(db_mutex: &Mutex<Database>) -> Result<VideoHashData> {
        let mut vhd = VideoHashData {
            hashes: Vec::new(),
            distances: Array::zeros((0, 0)),
        };
        vhd.refresh(db_mutex)?;
        Ok(vhd)
    }

    pub fn refresh(&mut self, db_mutex: &Mutex<Database>) -> Result<()> {
        // We do everything within the DB-mutex so concurrent calls work w/o races.
        if let Ok(db) = db_mutex.lock() {
            self.hashes = db.get_all_files_with_videohash()?;
            log::debug!("Num videohashs: {}", self.hashes.len());
            self.distances = videohash::calculate_distances(&self.hashes);
            log::debug!("Done with distance calculation");
        } else {
            return Err(anyhow!("Unable to lock DB"));
        }
        Ok(())
    }

    fn handle_request(&self, threshold: u16, tera: &Tera, allow_preview: bool) -> Result<Response> {
        log::debug!("# Clustering with threshold {}", threshold);
        let mut results = videohash::find_similar_files(&self.hashes, &self.distances, threshold);
        // sort by filesize (maximum first)
        let mut total_size_saved = 0;
        for bag in results.iter() {
            let mut max_size = 0;
            for f in bag {
                total_size_saved += f.size;
                max_size = std::cmp::max(max_size, f.size);
            }
            total_size_saved -= max_size;
        }
        let total_size_gb = total_size_saved as f64 / (1024.0 * 1024.0 * 1024.0);
        log::info!("Max saved size by videohash: {:.2} GB", total_size_gb);
        results.sort_unstable_by_key(|bag| bag.iter().map(|x| x.size).min());
        results.reverse();
        log::info!("# Clusters({}): {}", threshold, results.len());
        let html = render_videohash_results_to_html(results, &tera, allow_preview)?;
        Ok(Response::html(html))
    }
}

fn handle_rename_request(
    db_mutex: &Mutex<Database>,
    id: i64,
    new_name: String,
) -> Result<Response> {
    log::debug!("renaming {} to {}", id, new_name);
    if let Ok(db) = db_mutex.lock() {
        Ok(Response::text(rename_file(&db, id, new_name)?))
    } else {
        return Err(anyhow!("Unable to lock DB"));
    }
}

fn handle_remove_request(db_mutex: &Mutex<Database>, id: i64) -> Result<Response> {
    log::debug!("Deleting {}", id);
    if let Ok(db) = db_mutex.lock() {
        Ok(Response::text(delete_file(&db, id)?))
    } else {
        return Err(anyhow!("Unable to lock DB"));
    }
}

pub fn start_web_interface(
    db_mutex: Arc<Mutex<Database>>,
    bind_address: String,
    port: u16,
    allow_preview: bool,
) -> ! {
    if allow_preview && bind_address != "127.0.0.1" {
        log::warn!("You seem to be binding to a public interface and use --allow_preview.");
    }

    let tera = Tera::new("templates/**/*.html.tera").unwrap();
    let listen_address = format!("{}:{}", bind_address, port);
    let vhd_mutex = Arc::new(Mutex::new(
        VideoHashData::new(&Arc::clone(&db_mutex)).unwrap(),
    ));
    rouille::start_server(listen_address, move |request| {
        let db_mutex = Arc::clone(&db_mutex);
        let vhd_mutex = Arc::clone(&vhd_mutex);
        let response = router!(request,
            (GET) (/) => {handle_index_request(&db_mutex, &tera, allow_preview)},
            (GET) (/preview/{file_id: i64}) => {handle_preview_request(&db_mutex, file_id)},
            (GET) (/rename/{id: i64}/{new_name: String}) => {handle_rename_request(&db_mutex, id, new_name)},
            (GET) (/remove/{id: i64}) => {handle_remove_request(&db_mutex, id)},
            (GET) (/videohash/{threshold: u16}) => {
                vhd_mutex.lock().unwrap().handle_request(threshold, &tera, allow_preview)},
            (GET) (/refresh) => {
                let mut vhd = vhd_mutex.lock().unwrap();
                vhd.refresh(&db_mutex).unwrap();
                vhd.handle_request(1, &tera, allow_preview)
            },
            _ => Ok(Response::text("Unknown Request").with_status_code(500))
        );
        response.unwrap_or_else(|e| Response::text(e.to_string()).with_status_code(500))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::FileDigest;
    use std::path::PathBuf;

    #[test]
    fn test_rename_file() -> Result<()> {
        let db = Database::new("test3.sqlite", true)?;
        let file = FileDigest {
            id: 1,
            path: PathBuf::from("/tmp/a"),
            digest: vec![0, 1, 2, 3],
            size: 1,
        };
        db.insert_filedigest(&file)?;
        db.rename_file(1, "/tmp/b".to_string())?;
        let file = db.lookup_filedigest(1)?;
        assert_eq!(file.path.to_string_lossy(), "/tmp/b");
        Ok(())
    }
}
