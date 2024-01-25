use crate::*;

use futures_lite::StreamExt;
use ratelimit::Ratelimiter;
use splaycast::{Receiver, Sender};

use core::sync::atomic::Ordering;
use std::sync::Arc;

pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ratelimiter = config.ratelimiter();

    let runtime = config.runtime();

    let (tx, engine, splaycast) = splaycast::channel(config.queue_depth());

    {
        let _guard = runtime.fanout_runtime().enter();
        tokio::spawn(engine);
    }

    for _ in 0..config.subscribers() {
        runtime.spawn_subscriber(receiver(splaycast.subscribe()));
    }

    for _ in 0..config.publishers() {
        runtime.spawn_publisher(sender(config.clone(), tx.clone(), ratelimiter.clone()));
    }

    runtime.terminate();

    Ok(())
}

pub async fn receiver(mut rx: Receiver<Message>) {
    while RUNNING.load(Ordering::Relaxed) {
        match rx.next().await {
            Some(splaycast::Message::Entry { item }) => {
                let message = item;
                message.validate();

                RECV.increment();
                RECV_OK.increment();
            }
            Some(splaycast::Message::Lagged { count }) => {
                RECV.increment();
                RECV_OVERFLOW.increment();

                DROPPED.add(count.try_into().unwrap());
            }
            None => {
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
