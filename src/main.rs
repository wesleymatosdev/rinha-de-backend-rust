pub mod handlers;
pub mod payment_processor;

use axum::{Router, routing::post};

#[tokio::main]
async fn main() {
    let app = Router::new().route("/payments", post(|| {}));

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind TCP listener");

    println!("Server running on http://0.0.0.0:{port}");

    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
}
