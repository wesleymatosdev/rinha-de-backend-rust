use bytes::Bytes;

use async_nats::Client;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Payment {
    amount: f64,
    #[serde(rename = "correlationId")]
    correlation_id: String,
}

pub async fn publish_payment(client: Client, payment: Bytes) {
    client
        .publish("payments", payment)
        .await
        .expect("Failed to publish payment to NATS");
}

pub async fn dequeue_payment(client: Client) {
    // Use queue group to ensure each message is processed only once across instances
    let mut subscriber = client
        .queue_subscribe("payments", "payment-processors".into())
        .await
        .expect("Failed to subscribe to payments");

    while let Some(message) = subscriber.next().await {
        let payment: Payment =
            serde_json::from_slice(&message.payload).expect("Failed to deserialize payment");
        println!("Received payment from NATS: {:?}", payment);
    }
}
