use async_queues::mpmc;
use async_queues::mpmc::{Config, Test};
use clap::Parser;

fn main() {
    let config = Config::parse();

    match config.test() {
        Test::AsyncChannel => {
            let _ = mpmc::async_channel::run(config);
        }
        Test::Asyncstd => {
            let _ = mpmc::asyncstd::run(config);
        }
        Test::Flume => {
            let _ = mpmc::flume::run(config);
        }
        Test::Kanal => {
            let _ = mpmc::kanal::run(config);
        }
        Test::Postage => {
            let _ = mpmc::postage::run(config);
        }
    };
}
