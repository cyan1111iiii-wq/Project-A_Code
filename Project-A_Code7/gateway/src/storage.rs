use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::{Mutex, Arc, OnceLock};
use std::collections::HashMap;

// Fine-grained lock manager: Maintains a separate lock for each sensor_id.(by hashmap)
static LOCK_MANAGER: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn get_sensor_lock(sensor_id: &str) -> Arc<Mutex<()>> {
    let mut map = LOCK_MANAGER.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    map.entry(sensor_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

pub fn append(sensor_id: &str, line: &str) {
    // Acquire a dedicated lock for the current sensor without affecting the writing of other sensors.
    let sensor_lock = get_sensor_lock(sensor_id);
    let _guard = sensor_lock.lock().unwrap();

    let _ = fs::create_dir_all("data");
    let path = format!("data/{}.txt", sensor_id);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("Failed to open storage file");

    if let Err(e) = writeln!(file, "{}", line) {
        eprintln!("Couldn't write to file {}: {}", sensor_id, e);
    }
}