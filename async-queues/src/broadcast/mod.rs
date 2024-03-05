use crate::*;
use clap::Parser;
use ratelimit::Ratelimiter;
use std::sync::Arc;

pub mod async_broadcast;
pub mod broadcaster;
pub mod splaycast;
pub mod tokio;
pub mod widecast;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq)]
#[clap(rename_all = "snake_case")]
pub enum Test {
    AsyncBroadcast,
    Broadcaster,
    Splaycast,
    Tokio,
    Widecast,
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
    pub fn runtime(&self) -> Runtime<Self> {
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
    fn _runtime(&self, threads: usize) -> ::tokio::runtime::Runtime {
        ::tokio::runtime::Builder::new_multi_thread()
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
            write!(f, "publishers: {} publisher_threads: {} subscribers: {} subscriber_threads: {} fanout: {} fanout_threads: {} publish_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
                self.publishers,
                self.publisher_threads,
                self.subscribers,
                self.subscriber_threads,
                self.fanout,
                self.fanout_threads,
                self.publish_rate,
                self.queue_depth,
                SEND.value(), RECV_OK.value(), DROPPED.value())
        } else {
            write!(f, "publishers: {} subscribers: {} fanout: {} threads: {} publish_rate: {} queue_depth: {} send: {} recv: {} drop: {}",
                self.publishers,
                self.subscribers,
                self.fanout,
                self.threads,
                self.publish_rate,
                self.queue_depth,
                SEND.value(), RECV_OK.value(), DROPPED.value())
        }
    }
}
