use clap::{Parser, ValueEnum};
use histogram::SparseHistogram;
use metriken::{metric, AtomicHistogram, Counter};
use ratelimit::Ratelimiter;
use std::future::Future;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

pub mod async_broadcast;
pub mod broadcaster_broadcast;
pub mod splaycast_broadcast;
pub mod tokio_broadcast;
pub mod widecast_broadcast;

pub mod async_channel;
pub mod asyncstd_channel;
pub mod flume_channel;
pub mod kanal_channel;
pub mod postage_channel;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq)]
#[clap(rename_all = "snake_case")]
pub enum Test {
    AsyncBroadcast,
    BroadcasterBroadcast,
    SplaycastBroadcast,
    TokioBroadcast,
    WidecastBroadcast,
    AsyncChannel,
    AsyncstdChannel,
    FlumeChannel,
    KanalChannel,
    PostageChannel,
}

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

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    test: Test,

    #[arg(long, default_value_t = 60)]
    duration: u64,

    #[arg(long, default_value_t = 128)]
    queue_depth: usize,

    #[arg(long, default_value_t = false)]
    split_runtime: bool,

    #[arg(long, default_value_t = 64)]
    message_length: usize,

    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[arg(long, default_value_t = 61)]
    global_queue_interval: u32,

    #[arg(long, default_value_t = 61)]
    event_interval: u32,

    #[arg(long, default_value_t = 1)]
    publishers: usize,
    #[arg(long, default_value_t = 1)]
    publisher_threads: usize,

    #[arg(long, default_value_t = 1000)]
    publish_rate: u64,

    #[arg(long, default_value_t = 1)]
    subscribers: usize,
    #[arg(long, default_value_t = 1)]
    subscriber_threads: usize,

    #[arg(long, default_value_t = 0)]
    fanout: u8,
    #[arg(long, default_value_t = 1)]
    fanout_threads: usize,

    #[arg(long, default_value = None)]
    histogram: Option<String>,
}

impl Config {
    pub fn test(&self) -> Test {
        self.test
    }

    /// Create a ratelimiter for message sending based on the config
    pub fn ratelimiter(&self) -> Arc<Option<Ratelimiter>> {
        if self.publish_rate == 0 {
            return Arc::new(None);
        }

        let quanta = (self.publish_rate / 1_000_000) + 1;
        let delay = quanta * Duration::from_secs(1).as_nanos() as u64 / self.publish_rate;

        Arc::new(Some(
            Ratelimiter::builder(quanta, Duration::from_nanos(delay))
                .max_tokens(quanta)
                .build()
                .unwrap(),
        ))
    }

    /// Return the queue depth to be used for the channel/queue
    pub fn queue_depth(&self) -> usize {
        self.queue_depth
    }

    pub fn subscribers(&self) -> usize {
        self.subscribers
    }

    pub fn publishers(&self) -> usize {
        self.publishers
    }

    pub fn fanout(&self) -> u8 {
        self.fanout
    }

    pub fn message_length(&self) -> usize {
        self.message_length
    }

    /// Creates a collection of tokio runtimes, either one combined runtime or
    /// a dual runtime depending on the configuration
    pub fn runtime(&self) -> Runtime {
        let combined = self._runtime(self.threads);

        let publisher = if !self.split_runtime {
            None
        } else {
            Some(self._runtime(self.publisher_threads))
        };

        let subscriber = if !self.split_runtime {
            None
        } else {
            Some(self._runtime(self.subscriber_threads))
        };

        let fanout = if !self.split_runtime {
            None
        } else {
            Some(self._runtime(self.fanout_threads))
        };

        Runtime {
            config: self.clone(),
            combined,
            publisher,
            subscriber,
            fanout,
        }
    }

    /// Internal function to create a tokio runtime with a given number of
    /// threads. This makes sure we use consistent configuration for each
    /// runtime
    fn _runtime(&self, threads: usize) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .global_queue_interval(self.global_queue_interval)
            .event_interval(self.event_interval)
            .build()
            .expect("failed to initialize runtime")
    }
}

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

/// An abstraction for having either a combined runtime, or separate runtimes
/// for publishers and subscribers
pub struct Runtime {
    config: Config,
    combined: tokio::runtime::Runtime,
    publisher: Option<tokio::runtime::Runtime>,
    subscriber: Option<tokio::runtime::Runtime>,
    fanout: Option<tokio::runtime::Runtime>,
}

impl Runtime {
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

        std::thread::sleep(Duration::from_secs(self.config.duration));

        let stop = std::time::Instant::now();
        RUNNING.store(false, Ordering::Relaxed);

        std::thread::sleep(Duration::from_secs(1));

        println!(
            "global_queue_interval: {} event_interval: {}",
            self.config.global_queue_interval, self.config.event_interval
        );

        if self.config.split_runtime {
            println!("publishers: {} publisher_threads: {} subscribers: {} subscriber_threads: {} fanout: {} fanout_threads: {} publish_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
	            self.config.publishers,
	            self.config.publisher_threads,
	            self.config.subscribers,
	            self.config.subscriber_threads,
                self.config.fanout,
                self.config.fanout_threads,
	            self.config.publish_rate,
	            self.config.queue_depth,
	            SEND.value(), RECV_OK.value(), DROPPED.value());
        } else {
            println!("publishers: {} subscribers: {} fanout: {} threads: {} publish_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
	            self.config.publishers,
	            self.config.subscribers,
                self.config.fanout,
	            self.config.threads,
	            self.config.publish_rate,
	            self.config.queue_depth,
	            SEND.value(), RECV_OK.value(), DROPPED.value());
        }

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

            if let Some(path) = self.config.histogram {
                let sparse = SparseHistogram::from(&histogram);
                let json = serde_json::to_string(&sparse).expect("failed to serialize");
                let mut file = std::fs::File::create(path).expect("failed to create file");
                file.write_all(json.as_bytes())
                    .expect("failed to write to file");
            }
        }
    }
}
