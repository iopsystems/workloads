use crate::*;
use clap::Parser;
use ratelimit::Ratelimiter;
use std::sync::Arc;

pub mod async_channel;
pub mod asyncstd;
pub mod flume;
pub mod kanal;
pub mod postage;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq)]
#[clap(rename_all = "snake_case")]
pub enum Test {
    AsyncChannel,
    Asyncstd,
    Flume,
    Kanal,
    Postage,
}

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
    producers: usize,
    #[arg(long, default_value_t = 1)]
    producer_threads: usize,

    #[arg(long, default_value_t = 1000)]
    producer_rate: u64,

    #[arg(long, default_value_t = 1)]
    consumers: usize,
    #[arg(long, default_value_t = 1)]
    consumer_threads: usize,

    #[arg(long, default_value = None)]
    histogram: Option<String>,
}

impl Config {
    pub fn test(&self) -> Test {
        self.test
    }

    /// Create a ratelimiter for message sending based on the config
    pub fn ratelimiter(&self) -> Arc<Option<Ratelimiter>> {
        if self.producer_rate == 0 {
            return Arc::new(None);
        }

        let quanta = (self.producer_rate / 1_000_000) + 1;
        let delay = quanta * Duration::from_secs(1).as_nanos() as u64 / self.producer_rate;

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

    pub fn consumers(&self) -> usize {
        self.consumers
    }

    pub fn producers(&self) -> usize {
        self.producers
    }

    pub fn message_length(&self) -> usize {
        self.message_length
    }

    /// Creates a collection of tokio runtimes, either one combined runtime or
    /// a dual runtime depending on the configuration
    pub fn runtime(&self) -> Runtime<Self> {
        let combined = self._runtime(self.threads);

        let publisher = if !self.split_runtime {
            None
        } else {
            Some(self._runtime(self.producer_threads))
        };

        let subscriber = if !self.split_runtime {
            None
        } else {
            Some(self._runtime(self.consumer_threads))
        };

        Runtime {
            config: self.clone(),
            combined,
            publisher,
            subscriber,
            fanout: None,
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

impl AsyncQueuesConfig for Config {
    fn duration(&self) -> std::time::Duration {
        core::time::Duration::from_secs(self.duration)
    }

    fn histogram(&self) -> std::option::Option<&str> {
        self.histogram.as_deref()
    }

    fn global_queue_interval(&self) -> u32 {
        self.global_queue_interval
    }

    fn event_interval(&self) -> u32 {
        self.event_interval
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.split_runtime {
            write!(f, "producers: {} producer_threads: {} consumers: {} consumer_threads: {} producer_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
                self.producers,
                self.producer_threads,
                self.consumers,
                self.consumer_threads,
                self.producer_rate,
                self.queue_depth,
                SEND.value(), RECV_OK.value(), DROPPED.value())
        } else {
            write!(f, "producers: {} consumers: {} threads: {} producer_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
                self.producers,
                self.consumers,
                self.threads,
                self.producer_rate,
                self.queue_depth,
                SEND.value(), RECV_OK.value(), DROPPED.value())
        }
    }
}
