use std::fs::{self, read, File};
use std::io::{self, BufRead, BufReader, BufWriter};
use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use flate2::read::GzDecoder;
use rusqlite::{params, Connection, Result};
use serde_json::Value;

use anyhow::Result as AnyhowResult;
use reqwest::blocking::Client;
use std::io::copy;
use std::path::PathBuf;

fn start_amplitude_download(
    api_key: &str,
    secret_key: &str,
    start: &str,
    end: &str,
    output: &str,
) -> AnyhowResult<()> {
    // Build URL
    let url = format!(
        "https://amplitude.com/api/2/export?start={}&end={}",
        start, end
    );

    // Create HTTP client
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap();

    // Send GET request with Basic Auth
    let response = client
        .get(&url)
        .basic_auth(api_key, Some(secret_key))
        .send()?
        .error_for_status()?; // Ensure non-2xx responses are errors

    // Write response body to file
    let mut file = File::create(output)?;
    let bytes = response.bytes()?;
    let mut content = bytes.as_ref();
    copy(&mut content, &mut file)?;

    println!("Export saved to {output}");
    Ok(())
}

// TODO: check that cleanup is executed when re-running
// TODO: better duplicate detection

#[derive(Debug)]
pub struct ParsedItem {
    pub user_id: Option<String>,
    pub screen_name: Option<String>,
    pub event_name: String,
    pub server_event: bool,
    pub event_time: chrono::DateTime<Utc>,
    pub uuid: String,
    pub raw_json: String,
    pub source_file: String,
    pub session_id: Option<u64>,
}

// Unzips all `.gz` files in a source directory into a destination directory
pub fn unzip_gz_files(src_dir: &Path, dst_dir: &Path) -> io::Result<Vec<String>> {
    fs::create_dir_all(dst_dir)?;
    let mut processed_files = Vec::new();

    for entry in fs::read_dir(src_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("gz") {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let output_name = path.file_stem().unwrap().to_string_lossy().to_string();
            let dst_file_path = dst_dir.join(&output_name);

            let input_file = File::open(&path)?;
            let mut decoder = GzDecoder::new(BufReader::new(input_file));
            let output_file = File::create(dst_file_path)?;
            let mut writer = BufWriter::new(output_file);

            io::copy(&mut decoder, &mut writer)?;
            processed_files.push(file_name);
        }
    }

    Ok(processed_files)
}

// Parses all JSON lines from files in a directory
pub fn parse_json_objects_in_dir(dir: &Path) -> io::Result<Vec<ParsedItem>> {
    let mut results = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            for line_result in reader.lines() {
                let line = line_result?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let json: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to parse JSON in {}: {}", file_name, e);
                        continue;
                    }
                };

                let user_id = json
                    .get("user_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));

                let uuid = json
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing uuid"))?
                    .to_string();

                let server_event: bool = json
                    .get("data")
                    .unwrap()
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Missing data/path for server_event",
                        )
                    })?
                    .to_string()
                    != "/";
                let event_time: chrono::DateTime<Utc> = json
                    .get("event_time")
                    .map(|v| {
                        chrono::DateTime::parse_from_str(
                            &format!("{} +0000", v.as_str().unwrap().to_owned()),
                            "%Y-%m-%d %H:%M:%S%.6f %z",
                        )
                        .unwrap()
                        .to_utc()
                    })
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing event time"))
                    .unwrap();
                let event_name: String = json
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Missing event name")
                    })?
                    .to_string();
                let session_id: Option<u64> = json.get("session_id").and_then(|v| match v {
                    Value::Null => None,
                    Value::Bool(_) => None,
                    Value::Number(number) => number.as_u64(),
                    Value::String(_) => None,
                    Value::Array(_values) => None,
                    Value::Object(_map) => None,
                });
                let screen_name: Option<String> = None;
                results.push(ParsedItem {
                    user_id,
                    uuid,
                    event_name,
                    server_event,
                    event_time,
                    screen_name,
                    session_id,
                    raw_json: trimmed.to_string(),
                    source_file: file_name.clone(),
                });
            }
        }
    }

    Ok(results)
}

// Writes parsed items to a SQLite DB, avoiding duplicates and tracking import metadata
pub fn write_parsed_items_to_sqlite<P: AsRef<Path>>(
    db_path: P,
    items: &[ParsedItem],
    processed_files: &[String],
) -> Result<()> {
    let mut conn = Connection::open(db_path)?;

    // TODO: check that cleanup is executed when re-running
    // TODO: better duplicate detection

    // Ensure required tables exist
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS amplitude_events (
            uuid TEXT PRIMARY KEY,
            user_id TEXT,
            event_screen TEXT,
            server_event INTEGER,
            event_time DATETIME NOT NULL,
            event_name TEXT NOT NULL,
            session_id INTEGER,
            raw_json TEXT NOT NULL,
            source_file TEXT NOT NULL,
            created_at DATETIME NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imported_files (
            filename TEXT PRIMARY KEY,
            imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;

    let tx = conn.transaction()?;

    // Mark files as imported
    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO imported_files (filename) VALUES (?1)")?;
        for filename in processed_files {
            stmt.execute(params![filename])?;
        }
    }

    let mut inserted = 0;
    {
        // Insert parsed items
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO amplitude_events (uuid, user_id, raw_json, source_file, created_at, event_screen, server_event, event_time, event_name, session_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;

        for item in items {
            let rows = stmt.execute(params![
                item.uuid,
                item.user_id.as_deref(),
                item.raw_json,
                item.source_file,
                Utc::now().to_rfc3339(),
                item.screen_name,
                if item.server_event { 1 } else { 0 },
                item.event_time.to_rfc3339(),
                item.event_name,
                item.session_id,
            ])?;
            inserted += rows;
        }
    }

    tx.commit()?;

    println!(
        "Inserted {} new items. Skipped {} duplicates.",
        inserted,
        items.len() - inserted
    );

    Ok(())
}

// Reads filenames already processed (recorded in imported_files)
fn already_imported(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("SELECT filename FROM imported_files")?;
    let rows = stmt.query_map([], |row| row.get(0))?;

    let mut set = std::collections::HashSet::new();
    for filename in rows {
        set.insert(filename?);
    }
    Ok(set)
}

fn unzip_file(
    zip_file_path: &str,
    extract_to_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = fs::File::open(zip_file_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = match file.enclosed_name() {
            Some(path) => PathBuf::from(extract_to_path).join(path),
            None => continue,
        };

        if (*file.name()).ends_with('/') {
            // It's a directory, create it
            fs::create_dir_all(&outpath)?;
        } else {
            // It's a file, create parent directories and then the file
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(p)?;
                }
            }
            let mut outfile = fs::File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
        }

        // Set permissions if available
        #[cfg(unix)]
        {
            if let Some(mode) = file.unix_mode() {
                use std::os::unix::fs::PermissionsExt;

                fs::set_permissions(&outpath, fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Amplitude project API key (or set AMPLITUDE_PROJECT_API_KEY env var)
    #[arg(long, env = "AMPLITUDE_PROJECT_API_KEY")]
    api_key: String,

    /// Amplitude project secret key (or set AMPLITUDE_PROJECT_SECRET_KEY env var)
    #[arg(long, env = "AMPLITUDE_PROJECT_SECRET_KEY")]
    secret_key: String,

    /// Start date in format YYYYMMDDTHH (e.g., 20250101T00)
    #[arg(long)]
    start_date: String,

    /// End date in format YYYYMMDDTHH (e.g., 20251022T23)
    #[arg(long)]
    end_date: String,


    /// Project ID
    #[arg(long)]
    project_id: String,
}

// Main application entry point
fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let output = "amplitude_export.zip";

    start_amplitude_download(&args.api_key, &args.secret_key, &args.start_date, &args.end_date, &output).unwrap();
    unzip_file(&output, ".").unwrap();

    let compressed_dir = Path::new(&args.project_id);
    let unzipped_dir = Path::new("./data");
    let db_path = Path::new("amplitude_data.sqlite");

    // Open SQLite connection early to check for already-imported files
    let conn = Connection::open(db_path).expect("Failed to open DB");
    let imported_files = already_imported(&conn).unwrap_or_default();

    println!("Unzipping .gz files...");
    let all_gz_files = unzip_gz_files(compressed_dir, unzipped_dir)?;

    // Filter only new files that haven’t been imported
    let new_files: Vec<_> = all_gz_files
        .into_iter()
        .filter(|f| !imported_files.contains(f))
        .collect();

    if new_files.is_empty() {
        println!("No new files to process.");
        return Ok(());
    }

    println!("Parsing JSON lines...");
    let parsed_items = parse_json_objects_in_dir(unzipped_dir)?;

    println!("Writing parsed items to database...");
    write_parsed_items_to_sqlite(db_path, &parsed_items, &new_files)
        .expect("Failed to write to SQLite");

    println!("Done.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_end_to_end_multiple_files_and_rows() {
        fn create_gzipped_fixture(dir: &Path, name: &str, contents: &str) -> std::io::Result<()> {
            let path = dir.join(name);
            let file = File::create(path)?;
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut writer = BufWriter::new(encoder);
            writer.write_all(contents.as_bytes())?;
            writer.flush()?;
            Ok(())
        }

        let compressed_dir = tempdir().unwrap();
        let unzipped_dir = tempdir().unwrap();
        let db_path = compressed_dir.path().join("test_multiple.sqlite");

        // Two gzip files, each with 2 JSON objects
        let fixture1 = r#"
{ "user_id": "abc", "uuid": "uuid-0001", "data": {"path": "/test"}, "event_time": "2024-01-01 12:00:00.000000", "event_type": "test_event" }
{ "user_id": null, "uuid": "uuid-0002", "data": {"path": "/"}, "event_time": "2024-01-01 12:01:00.000000", "event_type": "test_event" }
"#;

        let fixture2 = r#"
{ "user_id": "def", "uuid": "uuid-0003", "data": {"path": "/test"}, "event_time": "2024-01-01 12:02:00.000000", "event_type": "test_event" }
{ "user_id": "ghi", "uuid": "uuid-0004", "data": {"path": "/"}, "event_time": "2024-01-01 12:03:00.000000", "event_type": "test_event" }
"#;

        create_gzipped_fixture(compressed_dir.path(), "fixture1.gz", fixture1)
            .expect("Failed fixture1");
        create_gzipped_fixture(compressed_dir.path(), "fixture2.gz", fixture2)
            .expect("Failed fixture2");

        // Unzip all .gz files
        let processed_files = unzip_gz_files(compressed_dir.path(), unzipped_dir.path())
            .expect("Failed to unzip files");

        // Parse all JSON lines from unzipped files
        let parsed_items = parse_json_objects_in_dir(unzipped_dir.path()).expect("Failed to parse");

        // Write parsed data to SQLite
        write_parsed_items_to_sqlite(&db_path, &parsed_items, &processed_files)
            .expect("Failed to write to SQLite");

        // Verify SQLite contents
        let conn = Connection::open(&db_path).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT uuid, user_id, raw_json, source_file FROM amplitude_events ORDER BY uuid",
            )
            .unwrap();

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap();

        let results: Vec<_> = rows.map(|r| r.unwrap()).collect();

        // Expect 4 rows total
        assert_eq!(results.len(), 4);

        // Check some values for correctness and ordering by uuid
        assert_eq!(results[0].0, "uuid-0001");
        assert_eq!(results[0].1.as_deref(), Some("abc"));
        assert!(results[0].2.contains("\"data\": {\"path\": \"/test\"}"));
        assert!(results[0].3.contains("fixture1"));

        assert_eq!(results[1].0, "uuid-0002");
        assert_eq!(results[1].1, None);
        assert!(results[1].2.contains("\"data\": {\"path\": \"/\"}"));
        assert!(results[1].3.contains("fixture1"));

        assert_eq!(results[2].0, "uuid-0003");
        assert_eq!(results[2].1.as_deref(), Some("def"));
        assert!(results[2].2.contains("\"data\": {\"path\": \"/test\"}"));
        assert!(results[2].3.contains("fixture2"));

        assert_eq!(results[3].0, "uuid-0004");
        assert_eq!(results[3].1.as_deref(), Some("ghi"));
        assert!(results[3].2.contains("\"data\": {\"path\": \"/\"}"));
        assert!(results[3].3.contains("fixture2"));
    }
}
