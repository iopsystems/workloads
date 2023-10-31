use crate::*;

use kanal::{AsyncReceiver, AsyncSender};
use ratelimit::Ratelimiter;

use core::sync::atomic::Ordering;
use std::sync::Arc;

pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ratelimiter = config.ratelimiter();

    let (tx, rx) = kanal::bounded_async(config.queue_depth());

    let runtime = config.runtime();

    for _ in 0..config.subscribers() {
        runtime.spawn_subscriber(receiver(rx.clone()));
    }

    for _ in 0..config.publishers() {
        runtime.spawn_publisher(sender(config.clone(), tx.clone(), ratelimiter.clone()));
    }

    runtime.terminate();

    Ok(())
}

pub async fn receiver(rx: AsyncReceiver<Message>) {
    while RUNNING.load(Ordering::Relaxed) {
        match rx.recv().await {
            Ok(message) => {
                message.validate();

                RECV.increment();
                RECV_OK.increment();
            }
            Err(_) => {
                RECV.increment();
                RECV_EX.increment();
            }
        }
    }
}

pub async fn sender(
    config: Config,
    tx: AsyncSender<Message>,
    ratelimiter: Arc<Option<Ratelimiter>>,
) {
    while !RUNNING.load(Ordering::Relaxed) {
        std::hint::spin_loop()
    }

    while RUNNING.load(Ordering::Relaxed) {
        if let Some(ratelimiter) = ratelimiter.as_ref() {
            if let Err(delay) = ratelimiter.try_wait() {
                tokio::time::sleep(delay).await;
                continue;
            }
        }

        let message = Message::new(config.message_length());

        if tx.send(message).await.is_ok() {
            SEND.increment();
            SEND_OK.increment();

            SEND_BYTES.add(config.message_length() as u64);
        } else {
            SEND.increment();
            SEND_EX.increment();

            DROPPED.increment();
        }
    }
}
