use crate::*;

use ratelimit::Ratelimiter;
use widecast::*;

use core::sync::atomic::Ordering;
use std::sync::Arc;

pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ratelimiter = config.ratelimiter();

    let runtime = config.runtime();

    // note: broadcaster's channels always have `overflow` behavior where
    // lagging subscribers will see messages dropped
    let tx = widecast::Sender::<Message>::new(config.queue_depth());

    for _ in 0..config.subscribers() {
        runtime.spawn_subscriber(receiver(tx.subscribe()));
    }

    let tx = Arc::new(tokio::sync::Mutex::new(tx));

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

pub async fn sender(
    config: Config,
    tx: Arc<tokio::sync::Mutex<Sender<Message>>>,
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

        let mut sender = tx.lock().await;
        sender.send(message);
        SEND.increment();
        SEND_OK.increment();

        SEND_BYTES.add(config.message_length() as u64);
    }
}
