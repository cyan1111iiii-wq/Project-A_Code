use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

pub struct BlockingBuffer<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
    capacity: usize,
    total_push: Mutex<usize>,
    total_pop: Mutex<usize>,
}

impl<T> BlockingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            condvar: Condvar::new(),
            capacity,
            total_push: Mutex::new(0),
            total_pop: Mutex::new(0),
        }
    }

    // Blocks when full, guarantees zero data loss
    pub fn push(&self, item: T) {
        let mut q = self.queue.lock().unwrap();

        while q.len() >= self.capacity {
            q = self.condvar.wait(q).unwrap();
        }

        q.push_back(item);
        *self.total_push.lock().unwrap() += 1;
        self.condvar.notify_all();
    }

    // Blocking read
    pub fn pop(&self) -> Option<T> {
        let mut q = self.queue.lock().unwrap();

        while q.is_empty() {
            q = self.condvar.wait(q).unwrap();
        }

        let item = q.pop_front();

        if item.is_some() {
            *self.total_pop.lock().unwrap() += 1;
        }

        self.condvar.notify_all();
        item
    }

    // Read with timeout
    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let start = Instant::now();
        let mut q = self.queue.lock().unwrap();

        while q.is_empty() {
            let remain = timeout.checked_sub(start.elapsed())?;
            let (guard, result) = self.condvar.wait_timeout(q, remain).unwrap();
            q = guard;

            if result.timed_out() && q.is_empty() {
                return None;
            }
        }

        let item = q.pop_front();

        if item.is_some() {
            *self.total_pop.lock().unwrap() += 1;
        }

        self.condvar.notify_all();
        item
    }

    pub fn utilization(&self) -> f64 {
        let q = self.queue.lock().unwrap();
        (q.len() as f64 / self.capacity as f64) * 100.0
    }

    pub fn throughput(&self) -> usize {
        *self.total_pop.lock().unwrap()
    }

    pub fn size(&self) -> usize {
        let q = self.queue.lock().unwrap();
        q.len()
    }

    pub fn total_pushed(&self) -> usize {
        *self.total_push.lock().unwrap()
    }

    pub fn total_popped(&self) -> usize {
    *self.total_pop.lock().unwrap()
}
}