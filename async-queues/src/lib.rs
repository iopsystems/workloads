use clap::ValueEnum;
use histogram::SparseHistogram;
use metriken::{metric, AtomicHistogram, Counter};
use std::future::Future;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

pub mod broadcast;
pub mod mpmc;

pub static RUNNING: AtomicBool = AtomicBool::new(true);

#[metric(name = "latency")]
pub static LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(name = "send")]
pub static SEND: Counter = Counter::new();

#[metric(name = "send_ok")]
pub static SEND_OK: Counter = Counter::new();

#[metric(name = "send_ex")]
pub static SEND_EX: Counter = Counter::new();

#[metric(name = "send_bytes")]
pub static SEND_BYTES: Counter = Counter::new();

#[metric(name = "recv")]
pub static RECV: Counter = Counter::new();

#[metric(name = "recv_ok")]
pub static RECV_OK: Counter = Counter::new();

#[metric(name = "recv_overflow")]
pub static RECV_OVERFLOW: Counter = Counter::new();

#[metric(name = "recv_ex")]
pub static RECV_EX: Counter = Counter::new();

#[metric(name = "recv_bytes")]
pub static RECV_BYTES: Counter = Counter::new();

#[metric(name = "dropped")]
pub static DROPPED: Counter = Counter::new();

#[derive(Clone)]
pub struct Message {
    timestamp: Instant,
    data: Vec<u8>,
}

impl Message {
    pub fn new(len: usize) -> Self {
        Self {
            timestamp: Instant::now(),
            data: vec![0; len],
        }
    }

    pub fn validate(&self) {
        let latency = self.timestamp.elapsed().as_nanos() as u64;
        let _ = LATENCY.increment(latency);

        RECV_BYTES.add(self.data.len() as u64);
    }
}

pub trait AsyncQueuesConfig {
    fn duration(&self) -> Duration;

    fn histogram(&self) -> Option<&str>;

    fn global_queue_interval(&self) -> u32;
    fn event_interval(&self) -> u32;
}

/// An abstraction for having either a combined runtime, or separate runtimes
/// for publishers and subscribers
pub struct Runtime<T> {
    config: T,
    combined: tokio::runtime::Runtime,
    publisher: Option<tokio::runtime::Runtime>,
    subscriber: Option<tokio::runtime::Runtime>,
    fanout: Option<tokio::runtime::Runtime>,
}

impl<T: AsyncQueuesConfig> Runtime<T> {
    pub fn spawn_publisher<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = self.publisher.as_ref().unwrap_or(&self.combined);

        rt.spawn(future)
    }

    pub fn spawn_subscriber<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = self.subscriber.as_ref().unwrap_or(&self.combined);

        rt.spawn(future)
    }

    pub fn fanout_runtime(&self) -> &tokio::runtime::Runtime {
        self.fanout.as_ref().unwrap_or(&self.combined)
    }

    pub fn terminate(self) {
        let start = std::time::Instant::now();
        RUNNING.store(true, Ordering::Relaxed);

        std::thread::sleep(self.config.duration());

        let stop = std::time::Instant::now();
        RUNNING.store(false, Ordering::Relaxed);

        std::thread::sleep(Duration::from_secs(1));

        println!(
            "global_queue_interval: {} event_interval: {}",
            self.config.global_queue_interval(),
            self.config.event_interval()
        );

        let elapsed = stop.duration_since(start).as_secs_f64();
        println!(
            "publish/s: {:.2} receive/s: {:.2} drop/s: {:.2}",
            SEND.value() as f64 / elapsed,
            RECV_OK.value() as f64 / elapsed,
            DROPPED.value() as f64 / elapsed
        );

        if let Some(histogram) = LATENCY.snapshot() {
            let latencies = histogram
                .percentiles(&[25.0, 50.0, 75.0, 90.0, 99.0, 99.9, 99.99])
                .unwrap();
            println!(
                "latency (µS): p25: {} p50: {} p75: {} p90: {} p99: {} p999: {} p9999: {}",
                latencies[0].1.end() / 1000,
                latencies[1].1.end() / 1000,
                latencies[2].1.end() / 1000,
                latencies[3].1.end() / 1000,
                latencies[4].1.end() / 1000,
                latencies[5].1.end() / 1000,
                latencies[6].1.end() / 1000,
            );

            if let Some(path) = self.config.histogram() {
                let sparse = SparseHistogram::from(&histogram);
                let json = serde_json::to_string(&sparse).expect("failed to serialize");
                let mut file = std::fs::File::create(path).expect("failed to create file");
                file.write_all(json.as_bytes())
                    .expect("failed to write to file");
            }
        }
    }
}
