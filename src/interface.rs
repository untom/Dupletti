use anyhow::Result;
use log;
use rouille::{router, Response};
use std::fs;
use tera::{Context as TeraContext, Tera};

use crate::database::Database;
use crate::similarities;
use std::sync::Mutex;

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

    println!(
        "Total saved size (GB): {:.3}",
        total_size_saved as f64 / (1024.0 * 1024.0 * 1024.0)
    );
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
