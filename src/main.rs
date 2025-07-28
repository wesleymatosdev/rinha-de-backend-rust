pub mod handlers;
pub mod payment_processor;

use std::sync::Arc;

use async_nats::{
    Client,
    jetstream::{self, Context},
};
use http_body_util::BodyExt;
use hyper::{Request, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioTimer;
// use axum::{
//     Router,
//     body::Bytes,
//     extract::{Query, State},
//     routing::{get, post},
// };
use sqlx::{Pool, postgres::PgPoolOptions};
// use viz::{
//     Bytes, Request, RequestExt, serve,
//     types::{Json, Query, State},
// };

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

    // let app = viz::Router::new()
    //     .post("/payments", async |mut req: viz::Request| {
    //         let State(state) = req.extract::<(State<AppState>)>().await?;

    //         let payment = req.bytes().await?;

    //         let _ = publish_payment(state.context.clone(), payment).await;

    //         Ok(())
    //     })
    //     .get("/payments-summary", async |mut req: viz::Request| {
    //         let (State(state), Query(filters)) = req
    //             .extract::<(State<AppState>, Query<PaymentSummaryFilters>)>()
    //             .await?;

    //         let response = payment_summary(filters, state.pg_pool.clone()).await;

    //         Ok(Json(response))
    //     })
    //     .post("/purge-payments", async |mut req: viz::Request| {
    //         let (State(state)) = req.extract::<(State<AppState>)>().await?;

    //         purge_payments(state.pg_pool.clone(), state.context.clone()).await;

    //         Ok(Json("Payments purged successfully"))
    //     })
    //     .with(State::new(AppState {
    //         context: context.clone(),
    //         pg_pool: pg_pool.clone(),
    //     }));

    // let app = Router::new()
    //     .route(
    //         "/payments",
    //         post(|state: State<AppState>, payment: Bytes| async move {
    //             let _ = publish_payment(state.context.clone(), payment).await;
    //         }),
    //     )
    //     .route(
    //         "/payments-summary",
    //         get(
    //             |state: State<AppState>, filters: Query<PaymentSummaryFilters>| async move {
    //                 let response = payment_summary(filters.0.clone(), state.pg_pool.clone()).await;

    //                 axum::Json(response)
    //             },
    //         ),
    //     )
    //     .route(
    //         "/purge-payments",
    //         post(|state: State<AppState>| async move {
    //             purge_payments(state.pg_pool.clone(), state.context.clone()).await;

    //             axum::Json("Payments purged successfully")
    //         }),
    //     )
    //     .with_state(AppState {
    //         pg_pool: pg_pool,
    //         context: context,
    //     });

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind TCP listener");

    // // let server = serve(listener, app).await;
    // if let Err(e) = serve(listener, app).await {
    //     eprintln!("Failed to start server: {}", e);
    //     std::process::exit(1);
    // }

    log::info!("Server running on http://0.0.0.0:{port}");
    let (stream, _) = listener
        .accept()
        .await
        .expect("Failed to accept connection");
    // let (tcp, _) = listener.accept().await;

    let io = hyper_util::rt::tokio::TokioIo::new(stream);

    let state = Arc::new(AppState {
        context: context.clone(),
        pg_pool: pg_pool.clone(),
    });

    tokio::task::spawn(async move {
        if let Err(err) = http1::Builder::new()
            .timer(TokioTimer::new())
            .serve_connection(
                io,
                service_fn(move |req| {
                    let state = state.clone();
                    async move { handler(req, state).await }
                }),
            )
            .await
        {
            eprintln!("Error serving connection: {}", err);
        }
    });

    // log::info!("Server running on http://0.0.0.0:{port}");

    // axum::serve(listener, app)
    //     .await
    //     .expect("Failed to start server");
}

async fn handler(
    req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
) -> Result<hyper::Response<String>, anyhow::Error> {
    match (req.method(), req.uri().path()) {
        (&hyper::Method::POST, "/payments") => {
            // let (parts, body) = req.into_parts();
            // let body = req.into_body();
            // let a = body.collect().await?;
            // let bytes = a.await.map_err(hyper::Error::from)?;
            // Handle payment publishing
            // publish_payment(state.context.clone(), a).await;
            Ok(hyper::Response::new("Payment published".to_string()))
        }
        (&hyper::Method::GET, "/payments-summary") => {
            let filters = req
                .uri()
                .query()
                .and_then(|query| serde_urlencoded::from_str::<PaymentSummaryFilters>(query).ok())
                .unwrap_or_default();

            // req.uri().query()
            // Handle payment summary
            // let filters = PaymentSummaryFilters::default(); // Replace with actual filter extraction logic
            let response = payment_summary(filters, state.pg_pool.clone()).await;
            let json_string =
                serde_json::to_string(&response).map_err(|e| anyhow::Error::from(e))?;
            Ok(hyper::Response::new(json_string))
        }
        (&hyper::Method::POST, "/purge-payments") => {
            // Handle purging payments
            purge_payments(state.pg_pool.clone(), state.context.clone()).await;
            Ok(hyper::Response::new(
                "Payments purged successfully".to_string(),
            ))
        }
        _ => Ok(hyper::Response::new("404 Not Found".to_string())),
    }
}
