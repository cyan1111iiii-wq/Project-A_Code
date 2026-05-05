# Real-Time Sensor Data Aggregation Platform

A Rust workspace-based **real-time multi-sensor streaming and
aggregation system**.

This project integrates:

-   real-time sensor simulation
-   lock-free queue communication
-   multi-threaded buffering
-   time-window aggregation
-   anomaly detection
-   persistent storage
-   RESTful APIs
-   live dashboard visualization

------------------------------------------------------------------------

# Run the Program

To start the full system, run:

``` bash
cargo run -p gateway
```

After startup:

-   **Dashboard Homepage:** `http://127.0.0.1:3000/`
-   **REST API Base:** `http://127.0.0.1:3001`

> Note: The dashboard runs on **port 3000**, while all data APIs run on
> **port 3001**.

------------------------------------------------------------------------

# Service Unified Addresses

## Dashboard Service

``` text
http://127.0.0.1:3000/
```

## API Service Base

``` text
http://127.0.0.1:3001
```

------------------------------------------------------------------------

# 1. Dashboard Homepage

## Real-time Visualization Homepage

``` text
http://127.0.0.1:3000/
```

### Features

-   Real-time sensor curves
-   Buffer usage monitoring
-   Throughput visualization
-   Dashboard charts
-   Auto-refresh every second

------------------------------------------------------------------------

# 2. Latest Data APIs

## Raw Latest Sensor Values

``` text
http://127.0.0.1:3001/latest/thermo-1
http://127.0.0.1:3001/latest/accel-1
http://127.0.0.1:3001/latest/force-1
```

## Aggregated Latest Results

``` text
http://127.0.0.1:3001/latest/aggregated_thermo-1
http://127.0.0.1:3001/latest/aggregated_accel-1
http://127.0.0.1:3001/latest/aggregated_force-1
```

### Returned Fields

-   `frame_id`
-   `timestamp`
-   `window_start`
-   `window_end`
-   `sensor_id`
-   `reading_count`
-   `min`
-   `max`
-   `avg`
-   `stddev`
-   `anomalies`

------------------------------------------------------------------------

# 3. Historical Last N Data APIs

## Raw Last 10 Records

``` text
http://127.0.0.1:3001/data/thermo-1/10
http://127.0.0.1:3001/data/accel-1/10
http://127.0.0.1:3001/data/force-1/10
```

## Aggregated Last 10 Records

``` text
http://127.0.0.1:3001/data/aggregated_thermo-1/10
http://127.0.0.1:3001/data/aggregated_accel-1/10
http://127.0.0.1:3001/data/aggregated_force-1/10
```

### Use Cases

-   verify recent sensor stream quality
-   inspect aggregation history
-   validate anomaly detection
-   grading demonstrations

------------------------------------------------------------------------

# 4. Statistics APIs

## Raw Sensor Statistics

``` text
http://127.0.0.1:3001/stats/thermo-1
http://127.0.0.1:3001/stats/accel-1
http://127.0.0.1:3001/stats/force-1
```

## Aggregated Statistics

``` text
http://127.0.0.1:3001/stats/aggregated_thermo-1
http://127.0.0.1:3001/stats/aggregated_accel-1
http://127.0.0.1:3001/stats/aggregated_force-1
```

### Returns

-   `min`
-   `max`
-   `avg`
-   `stddev`

------------------------------------------------------------------------

# 5. Buffer Monitoring API

## Buffer Status

``` text
http://127.0.0.1:3001/buffer_stats
```

### Example Response

``` json
{
  "utilization": 12.5,
  "throughput": 289
}
```

### Monitoring Purpose

Used to demonstrate:

-   queue pressure
-   system throughput
-   backpressure behavior
-   concurrency efficiency

------------------------------------------------------------------------

# 6. Registered Sensors API

## All Sensors

``` text
http://127.0.0.1:3001/registered_sensors
```

### Sensor List

-   `thermo-1`
-   `accel-1`
-   `force-1`
-   `aggregated_thermo-1`
-   `aggregated_accel-1`
-   `aggregated_force-1`

------------------------------------------------------------------------

# API Endpoint Summary

  Category                       Count
  --------------------------- --------
  Dashboard                          1
  Latest Endpoints                   6
  Historical Data Endpoints          6
  Statistics Endpoints               6
  Buffer Endpoint                    1
  Sensor List Endpoint               1
  **Total**                     **21**

------------------------------------------------------------------------

# Workspace Architecture

``` text
project-root/
├── os_lib/         # Lock-free round queue
├── sensor_sim/     # Sensor generators
├── dashboard/      # Frontend dashboard
├── gateway/        # Main runtime + APIs
└── Cargo.toml      # Workspace root
```

------------------------------------------------------------------------

# System Data Pipeline

``` text
sensor_sim (multi-threaded producers)
        ↓
lock-free sensor queues
        ↓
gateway blocking buffer
        ↓
aggregation workers
        ↓
persistent storage
        ↓
REST APIs + dashboard
```

------------------------------------------------------------------------

# Core Technical Highlights

## Concurrency

-   multi-producer sensor threads
-   worker-based aggregation
-   blocking shared buffer
-   graceful Ctrl+C shutdown

## Systems Programming

-   unsafe Rust lock-free queue
-   atomic read/write indices
-   power-of-two ring optimization
-   overwrite-on-full queue strategy

## Real-Time Analytics

-   sliding time windows
-   min/max/avg/stddev
-   anomaly detection
-   throughput monitoring

------------------------------------------------------------------------

# Data Persistence

Stored under:

``` text
data/
```

Example files:

``` text
data/thermo-1.txt
data/accel-1.txt
data/force-1.txt
data/aggregated_thermo-1.txt
```

------------------------------------------------------------------------

# Recommended Demo Flow

For grading/demo, use this order:

1.  Run the program
2.  Open dashboard homepage
3.  verify raw latest data
4.  verify aggregated latest data
5.  inspect last 10 records
6.  open statistics APIs
7.  verify buffer stats
8.  show registered sensors
9.  demonstrate graceful shutdown

This best demonstrates:

-   real-time streaming
-   concurrency
-   aggregation correctness
-   dashboard integration
-   system observability
