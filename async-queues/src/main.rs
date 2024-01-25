use async_queues::*;
use clap::Parser;

fn main() {
    let config = Config::parse();

    match config.test() {
        Test::AsyncBroadcast => {
            let _ = async_broadcast::run(config);
        }
        Test::AsyncChannel => {
            let _ = async_channel::run(config);
        }
        Test::AsyncstdChannel => {
            let _ = asyncstd_channel::run(config);
        }
        Test::BroadcasterBroadcast => {
            let _ = broadcaster_broadcast::run(config);
        }
        Test::FlumeChannel => {
            let _ = flume_channel::run(config);
        }
        Test::KanalChannel => {
            let _ = kanal_channel::run(config);
        }
        Test::PostageChannel => {
            let _ = postage_channel::run(config);
        }
        Test::SplaycastBroadcast => {
            let _ = splaycast_broadcast::run(config);
        }
        Test::TokioBroadcast => {
            let _ = tokio_broadcast::run(config);
        }
        Test::WidecastBroadcast => {
            let _ = widecast_broadcast::run(config);
        }
    };
}
