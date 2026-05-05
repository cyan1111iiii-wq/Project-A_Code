use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

static FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn get_lock() -> &'static Mutex<()> {
    FILE_LOCK.get_or_init(|| Mutex::new(()))
}

pub fn append(sensor_id: &str, line: &str) {
    let _guard = get_lock().lock().unwrap();

    std::fs::create_dir_all("data").unwrap();
    let path = format!("data/{}.txt", sensor_id);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();

    writeln!(file, "{}", line).unwrap();
    file.flush().unwrap();
}