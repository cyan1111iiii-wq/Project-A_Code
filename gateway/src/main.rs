use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::fs;

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

    // Start aggregation engine
    let mut agg_engine = AggregationEngine::new(1000, 2, 2.0);
    agg_engine.start(buf.clone());

    // Initialize sensors
    let mut thermo = Thermometer::new("thermo-1".to_string(), 10);
    let mut accel = Accelerometer::new("accel-1".to_string(), 10);
    let mut force = ForceSensor::new("force-1".to_string(), 10);

    thermo.start();
    accel.start();
    force.start();

    // Temperature sensor producer
    {
        let buf = buf.clone();
        thread::spawn(move || loop {
            if let Some(r) = thermo.read() {
                let value_str = format!("{}", r.temperature_celsius);
                let line = format!("thermo-1,{}", value_str);
                let _ = buf.push(line);
                append("thermo-1", &value_str);
            }
            thread::sleep(Duration::from_millis(10));
        });
    }

    // Accelerometer sensor producer
    {
        let buf = buf.clone();
        thread::spawn(move || loop {
            if let Some(r) = accel.read() {
                let value_str = format!("{}", r.acceleration_x);
                let line = format!("accel-1,{}", value_str);
                let _ = buf.push(line);
                append("accel-1", &value_str);
            }
            thread::sleep(Duration::from_millis(10));
        });
    }

    // Force sensor producer
    {
        let buf = buf.clone();
        thread::spawn(move || loop {
            if let Some(r) = force.read() {
                let value_str = format!("{}", r.force_x);
                let line = format!("force-1,{}", value_str);
                let _ = buf.push(line);
                append("force-1", &value_str);
            }
            thread::sleep(Duration::from_millis(10));
        });
    }

    let stats_buf = buf.clone();

    ctrlc::set_handler(move || {
        println!(
            "Shutting down... buffer usage: {:.2}%, throughput: {}",
            stats_buf.utilization(),
            stats_buf.throughput()
        );
        std::process::exit(0);
    }).unwrap();

    // ========== WEB API SERVER (port 3001) ==========
    let api_buf = buf.clone();

    let api_app = Router::new()
        .route("/", get(handle_root))                      // 根路径（新增）
        .route("/latest", get(handle_latest_all))
        .route("/latest/:id", get(handle_latest))
        .route("/data/:id/:n", get(handle_data))
        .route("/stats", get(handle_system_stats))
        .route("/stats/:id", get(handle_stats))
        .route("/buffer_stats", get(handle_buffer_stats))
        .route("/registered_sensors", get(handle_registered_sensors))
        .with_state(api_buf);

    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:3001").await.unwrap();
    println!("📡 Web API running at http://127.0.0.1:3001");

    tokio::spawn(async move {
        axum::serve(api_listener, api_app).await.unwrap();
    });
    // ========== END WEB API ==========

    println!("Dashboard running at http://127.0.0.1:3000/");
    APP.clone().run().await;

    agg_engine.shutdown();
}

// ========== Web API Handler Functions ==========

/// GET / - API root endpoint with available endpoints list
async fn handle_root() -> Json<Value> {
    Json(json!({
        "service": "Sensor Data Aggregation Platform API",
        "version": "1.0",
        "endpoints": [
            "GET / - API information",
            "GET /latest - Latest aggregated data for all sensors",
            "GET /latest/:id - Latest data for specific sensor",
            "GET /data/:id/:n - Last N data points for a sensor",
            "GET /stats - Overall system statistics",
            "GET /stats/:id - Statistics for specific sensor",
            "GET /buffer_stats - Current buffer status",
            "GET /registered_sensors - List of all sensors"
        ],
        "dashboard": "http://127.0.0.1:3000/",
        "status": "success"
    }))
}

/// GET /latest - Returns latest aggregated data for all sensors
async fn handle_latest_all() -> Json<Value> {
    let sensors = ["thermo-1", "accel-1", "force-1"];
    let mut results = vec![];
    
    for sensor in sensors {
        let path = format!("data/aggregated_{}.txt", sensor);
        if let Ok(content) = fs::read_to_string(&path) {
            if let Some(last_line) = content.lines().last() {
                if let Ok(json) = serde_json::from_str::<Value>(last_line) {
                    results.push(json);
                }
            }
        }
    }
    
    Json(json!({
        "latest_frames": results,
        "count": results.len(),
        "status": "success"
    }))
}

/// GET /latest/:id - Returns latest data for a specific sensor
async fn handle_latest(Path(id): Path<String>) -> Json<Value> {
    let agg_path = format!("data/aggregated_{}.txt", id);
    if let Ok(content) = fs::read_to_string(&agg_path) {
        if let Some(last_line) = content.lines().last() {
            if let Ok(json) = serde_json::from_str::<Value>(last_line) {
                return Json(json!({
                    "sensor_id": id,
                    "data": json,
                    "type": "aggregated",
                    "status": "success"
                }));
            }
        }
    }
    
    let raw_path = format!("data/{}.txt", id);
    if let Ok(content) = fs::read_to_string(&raw_path) {
        let last_line = content.lines().last().unwrap_or("");
        return Json(json!({
            "sensor_id": id,
            "latest_value": last_line,
            "type": "raw",
            "status": "success"
        }));
    }
    
    Json(json!({
        "sensor_id": id,
        "error": "Sensor not found",
        "status": "error"
    }))
}

/// GET /data/:id/:n - Returns last N data points for a sensor
async fn handle_data(Path((id, n)): Path<(String, usize)>) -> Json<Value> {
    let file_path = format!("data/{}.txt", id);
    
    match fs::read_to_string(&file_path) {
        Ok(content) => {
            let lines: Vec<&str> = content.lines().collect();
            let last_n: Vec<String> = lines
                .iter()
                .rev()
                .take(n)
                .map(|s| s.to_string())
                .rev()
                .collect();
            
            Json(json!({
                "sensor_id": id,
                "count": last_n.len(),
                "data": last_n,
                "status": "success"
            }))
        }
        Err(_) => {
            Json(json!({
                "sensor_id": id,
                "error": "File not found",
                "status": "error"
            }))
        }
    }
}

/// GET /stats - Returns overall system statistics
async fn handle_system_stats(State(buf): State<Arc<BlockingBuffer<String>>>) -> Json<Value> {
    Json(json!({
        "buffer_utilization": buf.utilization(),
        "buffer_throughput": buf.throughput(),
        "total_pushed": buf.total_pushed(),
        "total_popped": buf.total_popped(),
        "sensors": ["thermo-1", "accel-1", "force-1"],
        "status": "success"
    }))
}

/// GET /stats/:id - Returns statistics for a specific sensor
async fn handle_stats(Path(id): Path<String>) -> Json<Value> {
    let file_path = format!("data/aggregated_{}.txt", id);
    
    match fs::read_to_string(&file_path) {
        Ok(content) => {
            let mut min_sum = 0.0;
            let mut max_sum = 0.0;
            let mut avg_sum = 0.0;
            let mut stddev_sum = 0.0;
            let mut count = 0;
            
            for line in content.lines() {
                if let Ok(frame) = serde_json::from_str::<Value>(line) {
                    if let Some(min) = frame.get("min").and_then(|v| v.as_f64()) {
                        min_sum += min;
                        max_sum += frame.get("max").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        avg_sum += frame.get("avg").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        stddev_sum += frame.get("stddev").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        count += 1;
                    }
                }
            }
            
            if count > 0 {
                Json(json!({
                    "sensor_id": id,
                    "frame_count": count,
                    "avg_min": min_sum / count as f64,
                    "avg_max": max_sum / count as f64,
                    "avg_avg": avg_sum / count as f64,
                    "avg_stddev": stddev_sum / count as f64,
                    "status": "success"
                }))
            } else {
                Json(json!({
                    "sensor_id": id,
                    "error": "No data found",
                    "status": "error"
                }))
            }
        }
        Err(_) => {
            Json(json!({
                "sensor_id": id,
                "error": "File not found",
                "status": "error"
            }))
        }
    }
}

/// GET /buffer_stats - Returns current buffer status
async fn handle_buffer_stats(State(buf): State<Arc<BlockingBuffer<String>>>) -> Json<Value> {
    Json(json!({
        "utilization": buf.utilization(),
        "throughput": buf.throughput(),
        "size": buf.size(),
        "capacity": 5000,
        "status": "success"
    }))
}

/// GET /registered_sensors - Returns list of all registered sensors
async fn handle_registered_sensors() -> Json<Value> {
    let sensors = vec![
        "thermo-1",
        "accel-1",
        "force-1",
        "aggregated_thermo-1",
        "aggregated_accel-1",
        "aggregated_force-1",
    ];
    
    Json(json!({
        "sensors": sensors,
        "count": sensors.len(),
        "status": "success"
    }))
}