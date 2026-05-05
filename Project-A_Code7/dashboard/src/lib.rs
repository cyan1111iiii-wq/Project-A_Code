use hotaru::prelude::*;
use hotaru::http::*;
use std::fs;

// Register APP
pub static APP: SApp = Lazy::new(|| {
    App::new()
        .binding("127.0.0.1:3000")
        .build()
});

// Home page
endpoint! {
    APP.url("/"),
    pub index<HTTP> {
        akari_render!("home.html")
    }
}

// Latest data

endpoint!{
    APP.url("/latest/<sensor_id>"),
    pub latest<HTTP> {
        let sensor_id = req.param("sensor_id").unwrap();
        let path = format!("data/{}.txt", sensor_id);

        let content = fs::read_to_string(&path).unwrap_or_default();
        let latest_line = content.lines().last().unwrap_or("no data");

        // Aggregated result
        if sensor_id.starts_with("aggregated_") {
            return akari_json!({
                sensor: sensor_id,
                frame: latest_line
            });
        }

        // Raw result
        akari_json!({
            sensor: sensor_id,
            value: latest_line
        })
    }
}


// Buffer stats
endpoint!{
    APP.url("/buffer_stats"),
    pub buffer_stats<HTTP> {
        let content = fs::read_to_string("data/buffer_stats.txt")
            .unwrap_or("0,0".to_string());

        let parts: Vec<&str> = content.split(',').collect();

        let utilization = parts
            .get(0)
            .unwrap_or(&"0")
            .parse::<f64>()
            .unwrap_or(0.0);

        let throughput = parts
            .get(1)
            .unwrap_or(&"0")
            .parse::<f64>()
            .unwrap_or(0.0);

        akari_json!({
            utilization: utilization,
            throughput: throughput
        })
    }
}


// Last N entries

endpoint!{
    APP.url("/data/<sensor_id>/<num_of_data>"),
    pub data<HTTP> {
        let sensor_id = req.param("sensor_id").unwrap();
        let num_of_data = req.param("num_of_data")
            .unwrap()
            .parse::<usize>()
            .unwrap_or(10);

        let path = format!("data/{}.txt", sensor_id);
        let content = fs::read_to_string(&path).unwrap_or_default();

        let mut lines: Vec<&str> = content.lines().collect();
        lines.reverse();

        let data: Vec<&str> = lines.into_iter()
            .take(num_of_data)
            .collect();

        akari_json!({
            sensor_name: sensor_id,
            data: data
        })
    }
}


// Registered sensors

endpoint!{
    APP.url("/registered_sensors"),
    pub dashboard<HTTP> {
        akari_json!({
            sensors: [
                "thermo-1",
                "accel-1",
                "force-1",
                "aggregated_thermo-1",
                "aggregated_accel-1",
                "aggregated_force-1"
            ]
        })
    }
}


// Stats

endpoint!{
    APP.url("/stats/<sensor_id>"),
    pub stats<HTTP> {
        let sensor_id = req.param("sensor_id").unwrap();
        let path = format!("data/{}.txt", sensor_id);

        let content = fs::read_to_string(&path).unwrap_or_default();

        let values: Vec<f64> = content.lines()
            .filter_map(|line| {
                if let Ok(v) = line.parse::<f64>() {
                    return Some(v);
                }

                line.split(',')
                    .last()
                    .and_then(|v| v.trim_matches(|c| c == '}' || c == ' ' || c == '\"')
                    .parse::<f64>()
                    .ok())
            })
            .collect();

        if values.is_empty() {
            return akari_json!({
                error: "no data"
            });
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = values.iter().sum();
        let avg = sum / values.len() as f64;

        let variance = values.iter()
            .map(|v| (v - avg).powi(2))
            .sum::<f64>() / values.len() as f64;

        let stddev = variance.sqrt();

        akari_json!({
            sensor: sensor_id,
            min: min,
            max: max,
            avg: avg,
            stddev: stddev
        })
    }
}

pub mod resource;