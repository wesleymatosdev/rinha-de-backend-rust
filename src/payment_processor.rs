use std::time::Duration;

use async_nats::jetstream::{Context, consumer, stream::Config};
use futures_util::TryStreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use recloser::{AsyncRecloser, Recloser};

fn default_timestamp() -> DateTime<Utc> {
    Utc::now() // Or any other default value you prefer
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Payment {
    pub amount: f64,
    #[serde(rename = "correlationId")]
    pub correlation_id: String,
    // requested at with default
    #[serde(rename = "requestedAt", default = "default_timestamp")]
    pub requested_at: DateTime<Utc>,
}

static DEFAULT_PAYMENT_URL: OnceLock<String> = OnceLock::new();
static FALLBACK_PAYMENT_URL: OnceLock<String> = OnceLock::new();

pub fn get_default_payment_url() -> &'static str {
    DEFAULT_PAYMENT_URL.get_or_init(|| {
        std::env::var("DEFAULT_PAYMENT_URL").unwrap_or_else(|_| "http://localhost:8001".to_string())
    })
}

pub fn get_fallback_payment_url() -> &'static str {
    FALLBACK_PAYMENT_URL.get_or_init(|| {
        std::env::var("FALLBACK_PAYMENT_URL")
            .unwrap_or_else(|_| "http://localhost:8002".to_string())
    })
}

fn pay(
    http_client: Client,
    payment: Payment,
) -> impl Fn(&str) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    move |url: &str| {
        let url = url.to_string();
        let http_client = http_client.clone();
        let payment = payment.clone();
        Box::pin(async move {
            let json_body = serde_json::to_string(&payment)
                .map_err(|e| anyhow::anyhow!("Failed to serialize payment body: {}", e))?;

            log::debug!("Sending payment to {}: {:?}", url, json_body);
            let response = http_client
                .post(format!("{}/payments", url))
                .json(&payment)
                .send()
                .await?;

            if response.status().is_success() {
                log::debug!("Payment processed successfully: {:?}", payment);
                Ok(())
            } else {
                log::error!("Failed on payment: {:?}", response);
                let data = response.text().await?;
                Err(anyhow::anyhow!(
                    "Failed to POST to /payments: {:?}, response: {}",
                    payment,
                    data
                ))
            }
        })
    }
}

#[derive(Debug, Clone)]
struct CircuitBreaker {
    default: Arc<AsyncRecloser>,
    fallback: Arc<AsyncRecloser>,
}

enum CallType {
    Default,
    Fallback,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            default: Arc::new(AsyncRecloser::from(Recloser::default())),
            fallback: Arc::new(AsyncRecloser::from(Recloser::default())),
        }
    }

    pub async fn handle_payment_request(
        &self,
        data: CallType,
        client: Client,
        payment: Payment,
    ) -> Result<(), recloser::Error<anyhow::Error>> {
        let payer = pay(client, payment);
        match data {
            CallType::Default => self.default.call(payer(get_default_payment_url())),
            CallType::Fallback => self.fallback.call(payer(get_fallback_payment_url())),
        }
        .await
    }
}

async fn process_payment(
    circuit_breaker: CircuitBreaker,
    http_client: Client,
    payment: Payment,
) -> Result<(), anyhow::Error> {
    let default_request = circuit_breaker
        .handle_payment_request(CallType::Default, http_client.clone(), payment.clone())
        .await;

    match default_request {
        Err(recloser::Error::Rejected) => {
            log::warn!(
                "Failed to process payment with default URL from circuit breaker, trying fallback. correlation_id: {}",
                payment.correlation_id
            );
        }
        Err(recloser::Error::Inner(ref e)) => {
            log::error!(
                "Failed to process payment with default URL, trying fallback. correlation_id: {}, error: {:?}",
                payment.correlation_id,
                e
            );
        }
        Ok(_) => {
            log::debug!("Payment processed successfully with default payment URL.");
            return Ok(());
        }
    };

    let fallback_request = circuit_breaker
        .handle_payment_request(CallType::Fallback, http_client.clone(), payment.clone())
        .await;

    match fallback_request {
        Err(recloser::Error::Rejected) => {
            log::warn!(
                "Failed to process payment on fallback URL from circuit breaker. correlation_id: {}",
                payment.correlation_id
            );
        }
        Err(recloser::Error::Inner(ref e)) => {
            log::error!(
                "Failed to process payment on fallback URL. correlation_id: {}, error: {:?}",
                payment.correlation_id,
                e
            );
        }
        Ok(_) => {
            log::error!("Payment processed successfully with default payment URL.");
            return Ok(());
        }
    }

    Err(anyhow::anyhow!(
        "Failed to process payment with both default and fallback URLs."
    ))
}

async fn get_stream(ctx: Context) -> consumer::pull::Stream {
    let stream = ctx
        .create_stream(Config {
            name: "payments".to_string(),
            subjects: vec!["payments".to_string()],
            ..Default::default()
        })
        .await
        .expect("Failed to create or get stream");

    let consumer = stream
        .get_or_create_consumer(
            "payment-processor",
            consumer::pull::Config {
                durable_name: Some("payment-processor".to_string()),
                replay_policy: consumer::ReplayPolicy::Instant,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to get or create consumer");

    consumer
        .messages()
        .await
        .expect("Failed to get messages from consumer")
}

pub async fn dequeue_payment(ctx: Context) {
    let http_client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    let circuit_breaker = CircuitBreaker::new();

    let stream = get_stream(ctx.clone()).await;
    stream
        .try_for_each_concurrent(1000, |message| {
            let http_client = http_client.clone();
            let circuit_breaker = circuit_breaker.clone();
            async move {
                let payment: Payment = serde_json::from_slice(&message.payload)
                    .expect("Failed to deserialize payment");

                match process_payment(circuit_breaker, http_client, payment).await {
                    Ok(_) => message.ack().await.expect("Failed to ack message"),
                    Err(_) => {
                        message
                            .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                                Duration::from_secs(5),
                            )))
                            .await
                            .expect("Failed to nack message");
                        log::error!("Failed to process payment, message will be retried.");
                    }
                }

                Ok(())
            }
        })
        .await
        .expect("Failed to stream messages");
}
