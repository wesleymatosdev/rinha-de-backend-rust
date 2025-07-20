use bytes::Bytes;

use async_nats::Client;
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
