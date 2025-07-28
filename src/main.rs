pub mod handlers;
pub mod payment_processor;

use async_nats::{
    Client,
    jetstream::{self, Context},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    routing::{get, post},
};
use sqlx::{Pool, postgres::PgPoolOptions};

use crate::{
    handlers::{PaymentSummaryFilters, payment_summary, publish_payment, purge_payments},
    payment_processor::dequeue_payment,
};

async fn get_nats_client() -> Client {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    async_nats::connect(nats_url)
        .await
        .expect("Failed to connect to NATS")
}

async fn create_pg_pool() -> Result<Pool<sqlx::Postgres>, sqlx::Error> {
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
    let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "myuser".to_string());
    let password = std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "mypassword".to_string());
    let database = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "payments".to_string());

    let connection_string = format!("postgres://{user}:{password}@{host}:{port}/{database}");

    PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .connect(&connection_string)
        .await
}

#[derive(Debug, Clone)]
struct AppState {
    context: Context,
    pg_pool: Pool<sqlx::Postgres>,
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    let pg_pool = create_pg_pool()
        .await
        .expect("Failed to create PostgreSQL Pool");

    let context = jetstream::new(get_nats_client().await);

    tokio::task::spawn({
        let context = context.clone();
        let pg_pool = pg_pool.clone();
        async move {
            let _dequeue = dequeue_payment(context, pg_pool).await;
        }
    });

    let app = Router::new()
        .route(
            "/payments",
            post(|state: State<AppState>, payment: Bytes| async move {
                let _ = publish_payment(state.context.clone(), payment).await;
            }),
        )
        .route(
            "/payments-summary",
            get(
                |state: State<AppState>, filters: Query<PaymentSummaryFilters>| async move {
                    let response = payment_summary(filters.0.clone(), state.pg_pool.clone()).await;

                    axum::Json(response)
                },
            ),
        )
        .route(
            "/purge-payments",
            post(|state: State<AppState>| async move {
                purge_payments(state.pg_pool.clone(), state.context.clone()).await;

                axum::Json("Payments purged successfully")
            }),
        )
        .with_state(AppState {
            pg_pool: pg_pool,
            context: context,
        });

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind TCP listener");

    log::info!("Server running on http://0.0.0.0:{port}");

    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
}
