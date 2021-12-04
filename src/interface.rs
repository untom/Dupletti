use crate::database::Database;
use crate::similarities;
use crate::videohistogram;
use anyhow::Result;
use log;
use rouille::{router, Response};
use rusqlite::params;
use std::fs;
use std::sync::Mutex;
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
    result: Vec<Vec<&videohistogram::VideoHistogram>>,
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

pub fn start_web_interface(
    db: Database,
    bind_address: String,
    port: u16,
    allow_preview: bool,
) -> ! {
    if allow_preview && bind_address != "127.0.0.1" {
        log::warn!("You seem to be binding to a public interface and use --allow_preview.");
    }

    let videohistograms = db.get_all_files_with_histogram().unwrap();
    log::debug!("Num Videohistograms: {}", videohistograms.len());
    let videohistograms_distances = videohistogram::calculate_distances(&videohistograms);
    log::debug!("Done with distance calculation");

    let tera = Tera::new("templates/**/*.html.tera").unwrap();
    let listen_address = format!("{}:{}", bind_address, port);
    let db_mutex = Mutex::new(db);
    rouille::start_server(listen_address, move |request| {
        if allow_preview {
            if let Some(request) = request.remove_prefix("/preview") {
                return rouille::match_assets(&request, "/");
            }
        }

        let response = router!(request,
            (GET) (/) => {
                if let Ok(db) = db_mutex.lock() {
                    let results = similarities::get_list_of_similar_files(&db).unwrap();
                    let html = render_results_to_html(&results, &tera, allow_preview).unwrap();
                    Response::html(html)
                } else {
                    Response::text("Render Error").with_status_code(500)
                }
            },

            (GET) (/videohistogram/{threshold: u16}) => {
                log::debug!("# Clustering with threshold {}", threshold);
                let mut results = videohistogram::find_similar_files(&videohistograms, &videohistograms_distances, threshold);
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
                println!("Total saved size: {:.2} GB", total_size_gb);
                results.sort_unstable_by_key(|bag| bag.iter().map(|x| x.size).max());
                results.reverse();
                log::info!("# Clusters({}): {}", threshold, results.len());
                let html = render_videohash_results_to_html(results, &tera, allow_preview).unwrap();
                Response::html(html)
            },

            (GET) (/rename/{id: i64}/{new_name: String}) => {
                log::debug!("renaming {} to {}", id, new_name);
                if let Ok(db) = db_mutex.lock() {
                    let response = match rename_file(&db, id, new_name) {
                        Ok(status) => Response::text(status),
                        Err(error) => Response::text(format!("Rename failure: {:?}", error)).with_status_code(500),
                    };
                    response
                } else {
                    Response::text("Unable to acquire DB lock").with_status_code(500)
                }
            },

            (GET) (/remove/{id: i64}) => {
                log::debug!("Deleting {}", id);
                if let Ok(db) = db_mutex.lock() {
                    let response = match delete_file(&db, id) {
                        Ok(status) => Response::text(status),
                        Err(error) => Response::text(format!("Delete failure: {:?}", error)).with_status_code(500),
                    };
                    response
                } else {
                    Response::text("Unable to acquire DB lock").with_status_code(500)
                }
            },

            // default route
            _ => Response::empty_404()
        );
        response
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
