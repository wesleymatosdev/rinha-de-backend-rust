pub mod payment_processor;

use async_nats::{
    Client,
    jetstream::{self, Context},
};
use axum::{Router, body::Bytes, extract::State, routing::post};

use crate::payment_processor::dequeue_payment;

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
    let context = jetstream::new(client);

    tokio::task::spawn({
        let context = context.clone();
        async move {
            let _dequeue = dequeue_payment(context).await;
        }
    });

    let app = Router::new()
        .route(
            "/payments",
            post(|context: State<Context>, payment: Bytes| async move {
                context
                    .publish("payments", payment)
                    .await
                    .expect("Failed to publish payment to NATS");
            }),
        )
        .with_state(context);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind TCP listener");

    println!("Server running on http://0.0.0.0:{port}");

    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
}
