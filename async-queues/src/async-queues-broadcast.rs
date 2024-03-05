use async_queues::broadcast;
use async_queues::broadcast::{Config, Test};
use clap::Parser;

fn main() {
    let config = Config::parse();

    match config.test() {
        Test::AsyncBroadcast => {
            let _ = broadcast::async_broadcast::run(config);
        }
        Test::Broadcaster => {
            let _ = broadcast::broadcaster::run(config);
        }
        Test::Splaycast => {
            let _ = broadcast::splaycast::run(config);
        }
        Test::Tokio => {
            let _ = broadcast::tokio::run(config);
        }
        Test::Widecast => {
            let _ = broadcast::widecast::run(config);
        }
    };
}
