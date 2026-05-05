use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering}; 

mod aggregation;
mod buffer;
mod storage;

use aggregation::AggregationEngine;
use buffer::BlockingBuffer;
use storage::append;
use dashboard::APP;

use sensor_sim::accelerometer::Accelerometer;
use sensor_sim::force_sensor::ForceSensor;
use sensor_sim::thermometer::Thermometer;
use sensor_sim::traits::Sensor;

// Web API dependencies
use axum::{
    Router,
    routing::get,
    Json,
    extract::{Path, State},
};
use serde_json::{json, Value};

#[tokio::main]
async fn main() {
    let buf = Arc::new(BlockingBuffer::new(5000));
    
    // Graceful Shutdown Signal Controller
    let running = Arc::new(AtomicBool::new(true));

    // Start the aggregation engine
    let mut agg_engine = AggregationEngine::new(1000, 2, 1.0);
    agg_engine.start(buf.clone());

    // Initialize the sensor
    let mut thermo = Thermometer::new("thermo-1".to_string(), 100);
    let mut accel = Accelerometer::new("accel-1".to_string(), 100);
    let mut force = ForceSensor::new("force-1".to_string(), 100);

    thermo.start();
    accel.start();
    force.start();

    // Producer：Supports dynamic scheduling and graceful exit
    
    // Temperature sensor producer
    {
        let buf = buf.clone();
        let r = running.clone();
        thread::spawn(move || {
            while r.load(Ordering::SeqCst) { // check shutdown signal
                if let Some(res) = thermo.read() {
                    let util = buf.utilization();
                    // overflow warning 
                    if util > 0.8 { eprintln!("[WARN] Thermo buffer saturation high: {:.1}%", util * 100.0); }
                    
                    let value_str = format!("{}", res.temperature_celsius);
                    let line = format!("thermo-1,{}", value_str);
                    buf.push(line);
                    append("thermo-1", &value_str);

                    // Dynamic scheduling logic: If the buffer is under heavy load, reduce sleep time and speed up processing.
                    let sleep_ms = if util > 0.7 { 2 } else { 10 };
                    thread::sleep(Duration::from_millis(sleep_ms));
                }
            }
            println!("Thermo producer exited.");
        });
    }

    // Accelerometer sensor producer
    {
        let buf = buf.clone();
        let r = running.clone();
        thread::spawn(move || {
            while r.load(Ordering::SeqCst) {
                if let Some(res) = accel.read() {
                    let util = buf.utilization();
                    if util > 0.8 { eprintln!("[WARN] Accel buffer saturation high!"); }
                    
                    let value_str = format!("{}", res.acceleration_x);
                    let line = format!("accel-1,{}", value_str);
                    buf.push(line);
                    append("accel-1", &value_str);

                    let sleep_ms = if util > 0.7 { 2 } else { 10 };
                    thread::sleep(Duration::from_millis(sleep_ms));
                }
            }
            println!("Accel producer exited.");
        });
    }

    // Force sensor producer
    {
        let buf = buf.clone();
        let r = running.clone();
        thread::spawn(move || {
            while r.load(Ordering::SeqCst) {
                if let Some(res) = force.read() {
                    let util = buf.utilization();
                    if util > 0.8 { eprintln!("[WARN] Force buffer saturation high!"); }
                    
                    let value_str = format!("{}", res.force_x);
                    let line = format!("force-1,{}", value_str);
                    buf.push(line);
                    append("force-1", &value_str);

                    let sleep_ms = if util > 0.7 { 2 } else { 10 };
                    thread::sleep(Duration::from_millis(sleep_ms));
                }
            }
            println!("Force producer exited.");
        });
    }

    //Ctrl-C processor: graceful shutdown without forced exit
    let stats_buf = buf.clone();
    let r_signal = running.clone();
    ctrlc::set_handler(move || {
        println!("\n[SHUTDOWN] Signal received. Flushing buffers and saving data...");
        r_signal.store(false, Ordering::SeqCst); // Notify all threads to stop production.
        
        println!(
            "Final Status - Utilization: {:.2}%, Throughput: {}",
            stats_buf.utilization(),
            stats_buf.throughput()
        );
      
    }).unwrap();

    // WEB API SERVER (3001) 
    let api_buf = buf.clone();
    let api_app = Router::new()
        .route("/", get(handle_root))
        .route("/latest", get(handle_latest_all))
        .route("/latest/:id", get(handle_latest))
        .route("/data/:id/:n", get(handle_data))
        .route("/stats", get(handle_system_stats))
        .route("/stats/:id", get(handle_stats))
        .route("/buffer_stats", get(handle_buffer_stats))
        .route("/registered_sensors", get(handle_registered_sensors))
        .with_state(api_buf);

    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:3001").await.unwrap();
    println!("Web API running at http://127.0.0.1:3001");

    tokio::spawn(async move {
        axum::serve(api_listener, api_app).await.unwrap();
    });

    println!("Dashboard running at http://127.0.0.1:3000/");
    
    // start Dashboard 
    APP.clone().run().await;

    // [Key Step] Close the components one by one to ensure no data is lost.
    println!("Finalizing aggregation...");
    agg_engine.shutdown(); 
    println!("System shutdown gracefully.");
}

//Web API Handler Functions 

async fn handle_root() -> Json<Value> {
    Json(json!({
        "service": "Sensor Data Aggregation Platform API",
        "endpoints": ["GET /", "GET /latest", "GET /data/:id/:n", "GET /stats"],
        "status": "success"
    }))
}

async fn handle_latest(Path(mut id): Path<String>) -> Json<Value> {
    let clean_id = id.replace("aggregated_", "");
    let agg_path = format!("data/aggregated_{}.txt", clean_id);
    if let Ok(content) = fs::read_to_string(&agg_path) {
        if let Some(last_line) = content.lines().last() {
            if let Ok(json_obj) = serde_json::from_str::<Value>(last_line) {
                return Json(json!({
                    "sensor_id": format!("aggregated_{}", clean_id),
                    "latest_data": json_obj,
                    "type": "aggregated",
                    "status": "success"
                }));
            }
        }
    }
    
    let raw_path = format!("data/{}.txt", id);
    if let Ok(content) = fs::read_to_string(&raw_path) {
        if let Some(last_line) = content.lines().last() {
            return Json(json!({
                "sensor_id": id,
                "latest_value": last_line,
                "type": "raw",
                "status": "success"
            }));
        }        
    }
    Json(json!({ "error": "Sensor not found", "status": "error" }))
}

async fn handle_data(Path((id, n)): Path<(String, usize)>) -> Json<Value> {
    let file_path = format!("data/{}.txt", id);
    match fs::read_to_string(&file_path) {
        Ok(content) => {
            let lines: Vec<&str> = content.lines().collect();
            let last_n: Vec<Value> = lines.iter().rev().take(n)
                .map(|s| serde_json::from_str::<Value>(s).unwrap_or(json!(s)))
                .rev().collect();
            
            Json(json!({ "sensor_id": id, "count": last_n.len(), "data": last_n, "status": "success" }))
        }
        Err(_) => Json(json!({ "error": "File not found", "status": "error" }))
    }
}

async fn handle_latest_all() -> Json<Value> {
    Json(json!({"status": "feature_pending", "message": "Global latest view not implemented"}))
}

async fn handle_system_stats(State(buf): State<Arc<BlockingBuffer<String>>>) -> Json<Value> {
    Json(json!({
        "buffer_utilization": buf.utilization(),
        "buffer_throughput": buf.throughput(),
        "total_pushed": buf.total_pushed(),
        "total_popped": buf.total_popped(),
        "status": "success"
    }))
}

async fn handle_stats(Path(id): Path<String>) -> Json<Value> {
    let file_path = format!("data/aggregated_{}.txt", id);
    if let Ok(content) = fs::read_to_string(&file_path) {
        let mut min_sum = 0.0;
        let mut count = 0;
        for line in content.lines() {
            if let Ok(frame) = serde_json::from_str::<Value>(line) {
                if let Some(min) = frame.get("min").and_then(|v| v.as_f64()) {
                    min_sum += min;
                    count += 1;
                }
            }
        }
        if count > 0 {
            return Json(json!({ "sensor_id": id, "avg_min_observed": min_sum / count as f64, "status": "success" }));
        }
    }
    Json(json!({ "error": "Data unavailable", "status": "error" }))
}

async fn handle_buffer_stats(State(buf): State<Arc<BlockingBuffer<String>>>) -> Json<Value> {
    Json(json!({
        "utilization": buf.utilization(),
        "throughput": buf.throughput(),
        "size": buf.size(),
        "capacity": 5000,
        "status": "success"
    }))
}

async fn handle_registered_sensors() -> Json<Value> {
    let sensors = vec!["thermo-1", "accel-1", "force-1"];
    Json(json!({ "sensors": sensors, "status": "success" }))
}