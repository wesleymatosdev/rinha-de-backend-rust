pub mod handlers;
pub mod payment_processor;

use crate::handlers::{dequeue_payment, publish_payment};
use async_nats::Client;
use axum::{Router, body::Bytes, extract::State, routing::post};

async fn get_nats_client() -> Client {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    async_nats::connect(nats_url)
        .await
        .expect("Failed to connect to NATS")
}

#[tokio::main]
async fn main() {
    let client = get_nats_client().await;

    tokio::task::spawn({
        let client = client.clone();
        async move { dequeue_payment(client).await }
    });

    let app = Router::new()
        .route(
            "/payments",
            post(|client: State<Client>, payment: Bytes| async move {
                publish_payment(client.0, payment).await;
            }),
        )
        .with_state(client);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind TCP listener");

    println!("Server running on http://0.0.0.0:{port}");

    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
}
