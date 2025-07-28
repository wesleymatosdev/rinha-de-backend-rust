pub mod payment_processor;

use std::collections::HashMap;

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
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, postgres::PgPoolOptions};

use crate::payment_processor::dequeue_payment;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PaymentSummaryFilters {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
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
                state
                    .context
                    .publish("payments", payment)
                    .await
                    .expect("Failed to publish payment to NATS");
            }),
        )
        .route(
            "/payments-summary",
            get(
                |state: State<AppState>, filters: Query<PaymentSummaryFilters>| async move {
                    let sql = r#"
                        SELECT correlation_id, amount, requested_at, gateway
                        FROM payments
                        WHERE payments.requested_at >= $1 AND payments.requested_at <= $2
                    "#;

                    let from = filters
                        .from
                        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));
                    let to = filters.to.unwrap_or_else(|| Utc::now());

                    let payments = sqlx::query_as::<_, (String, f64, DateTime<Utc>, String)>(sql)
                        .bind(from)
                        .bind(to)
                        .fetch_all(&state.pg_pool)
                        .await
                        .expect("Failed to fetch payment summary");
                    // }

                    let mut summary: HashMap<String, (i32, f64)> = HashMap::new();
                    for (_correlation_id, amount, _requested_at, gateway) in payments {
                        let entry = summary.entry(gateway).or_insert((0, 0.0));
                        entry.0 += 1; // totalRequests
                        entry.1 += amount as f64; // totalAmount
                    }

                    let empty_json = serde_json::json!({
                        "totalRequests": 0,
                        "totalAmount": 0.0
                    });
                    // solution above doesn't work as it does not have defaults
                    let default = summary
                        .get("default")
                        .map(|&(total_requests, total_amount)| {
                            serde_json::json!({
                                "totalRequests": total_requests,
                                "totalAmount": total_amount
                            })
                        })
                        .unwrap_or(empty_json.clone());
                    let fallback = summary
                        .get("fallback")
                        .map(|&(total_requests, total_amount)| {
                            serde_json::json!({
                                "totalRequests": total_requests,
                                "totalAmount": total_amount
                            })
                        })
                        .unwrap_or(empty_json);

                    axum::Json(serde_json::json!({
                        "default": default,
                        "fallback": fallback
                    }))
                },
            ),
        )
        .route(
            "/purge-payments",
            post(|state: State<AppState>| async move {
                let sql = "DELETE FROM payments";
                sqlx::query(sql)
                    .execute(&state.pg_pool)
                    .await
                    .expect("Failed to purge payments");

                // purge payments from NATS
                let stream = state
                    .context
                    .get_stream("payments")
                    .await
                    .expect("Failed to get payments stream");

                stream
                    .purge()
                    .await
                    .expect("Failed to purge payments from NATS");

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
