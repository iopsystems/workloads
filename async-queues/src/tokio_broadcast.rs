use crate::*;

use ratelimit::Ratelimiter;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{Receiver, Sender};

use core::sync::atomic::Ordering;
use std::sync::Arc;

pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ratelimiter = config.ratelimiter();

    // note: tokio broadcast channels always have `overflow` behavior where
    // lagging subscribers will see messages dropped
    let (tx, _rx) = tokio::sync::broadcast::channel::<Message>(config.queue_depth());

    let runtime = config.runtime();

    for _ in 0..config.subscribers() {
        runtime.spawn_subscriber(receiver(tx.subscribe()));
    }

    for _ in 0..config.publishers() {
        runtime.spawn_publisher(sender(config.clone(), tx.clone(), ratelimiter.clone()));
    }

    runtime.terminate();

    Ok(())
}

pub async fn receiver(mut rx: Receiver<Message>) {
    while RUNNING.load(Ordering::Relaxed) {
        match rx.recv().await {
            Ok(message) => {
                message.validate();

                RECV.increment();
                RECV_OK.increment();
            }
            Err(RecvError::Lagged(count)) => {
                RECV.increment();
                RECV_OVERFLOW.increment();

                DROPPED.add(count);
            }
            Err(_) => {
                RECV.increment();
                RECV_EX.increment();
            }
        }
    }
}

pub async fn sender(config: Config, tx: Sender<Message>, ratelimiter: Arc<Option<Ratelimiter>>) {
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

        // note: tokio broadcast channel send is not async for some reason
        if tx.send(message).is_ok() {
            SEND.increment();
            SEND_OK.increment();

            SEND_BYTES.add(config.message_length() as u64);
        } else {
            SEND.increment();
            SEND_EX.increment();
        }
    }
}
