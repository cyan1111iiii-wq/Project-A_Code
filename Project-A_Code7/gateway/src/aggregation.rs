use crate::buffer::BlockingBuffer;
use crate::storage::append;

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// AggregatedFrame struct (meets specification)
#[derive(Serialize, Debug, Clone)]
pub struct AggregatedFrame {
    pub frame_id: u64,
    pub timestamp: i64,
    pub window_start: i64,
    pub window_end: i64,
    pub sensor_id: String,
    pub reading_count: usize,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub stddev: f64,
    pub anomalies: Vec<f64>,
}

// Helper functions

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// Sensor window statistics
struct SensorWindow {
    count: usize,
    sum: f64,
    sum_sq: f64,
    min: f64,
    max: f64,
    values: Vec<f64>,
}

impl SensorWindow {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            values: Vec::new(),
        }
    }

    fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.values.push(value);
    }

    fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }

    fn stddev(&self) -> f64 {
        if self.count == 0 { return 0.0; }
        let avg = self.avg();
        let variance = (self.sum_sq / self.count as f64) - (avg * avg);
        if variance < 0.0 { 0.0 } else { variance.sqrt() }
    }

    fn anomalies(&self, threshold: f64) -> Vec<f64> {
        let avg = self.avg();
        let stddev = self.stddev();
        if stddev == 0.0 { return Vec::new(); }
        self.values
            .iter()
            .filter(|&&v| (v - avg).abs() > threshold * stddev)
            .copied()
            .collect()
    }
}

// AggregationEngine struct (meets specification)
pub struct AggregationEngine {
    window_ms: u64,
    workers: usize,
    threshold: f64,
    running: Arc<AtomicBool>,
    frame_counter: Arc<AtomicU64>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl AggregationEngine {
    pub fn new(window_ms: u64, workers: usize, threshold: f64) -> Self {
        Self {
            window_ms,
            workers,
            threshold,
            running: Arc::new(AtomicBool::new(true)),
            frame_counter: Arc::new(AtomicU64::new(0)),
            handles: Vec::new(),
        }
    }

    pub fn start(&mut self, buffer: Arc<BlockingBuffer<String>>) {
        let running = self.running.clone();
        let frame_counter = self.frame_counter.clone();
        let window_duration = Duration::from_millis(self.window_ms);
        let threshold = self.threshold;
        let workers = self.workers;

        for worker_id in 0..workers {
            let buffer_clone = buffer.clone();
            let running_clone = running.clone();
            let frame_counter_clone = frame_counter.clone();
            let window_duration_clone = window_duration;

            let handle = thread::spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    let window_start = Instant::now();
                    let window_start_ms = now_ms();
                    let mut windows: HashMap<String, SensorWindow> = HashMap::new();

                    // Collect data within a time window
                    while window_start.elapsed() < window_duration_clone {
                        // Use pop_timeout to avoid permanent blocking
                        if let Some(line) = buffer_clone.pop_timeout(Duration::from_millis(10)) {
                            let parts: Vec<&str> = line.split(',').collect();
                            if parts.len() >= 2 {
                                let sensor_id = parts[0].to_string();

                                let value = if parts.len() == 2 {
                                    parts[1].parse::<f64>().unwrap_or(0.0)
                                } else if parts.len() >= 4 {
                                    let x = parts[1].parse::<f64>().unwrap_or(0.0);
                                    let y = parts[2].parse::<f64>().unwrap_or(0.0);
                                    let z = parts[3].parse::<f64>().unwrap_or(0.0);
                                    (x * x + y * y + z * z).sqrt()
                                } else {
                                    continue;
                                };

                                windows.entry(sensor_id)
                                    .or_insert_with(SensorWindow::new)
                                    .add(value);
                            }
                        }

                        if !running_clone.load(Ordering::Relaxed) {
                            break;
                        }
                    }

                    let window_end_ms = now_ms();

                    // Output aggregation results
                    if !windows.is_empty() {
                        for (sensor_id, w) in windows.iter() {
                            let avg = w.avg();
                            let stddev = w.stddev();
                            let anomalies = w.anomalies(threshold);

                            let frame_id = frame_counter_clone.fetch_add(1, Ordering::Relaxed);

                            let frame = AggregatedFrame {
                                frame_id,
                                timestamp: window_end_ms,
                                window_start: window_start_ms,
                                window_end: window_end_ms,
                                sensor_id: sensor_id.clone(),
                                reading_count: w.count,
                                min: w.min,
                                max: w.max,
                                avg,
                                stddev,
                                anomalies,
                            };

                            let json = serde_json::to_string(&frame).unwrap();
                            append(&format!("aggregated_{}", sensor_id), &json);
                        }
                    }
                }
            });

            self.handles.push(handle);
        }
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}