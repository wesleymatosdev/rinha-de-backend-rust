use std::collections::HashMap;

use anyhow::Error;
use async_nats::jetstream::{Context, publish::PublishAck};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Pool;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PaymentSummaryFilters {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

pub async fn publish_payment(context: Context, payment: bytes::Bytes) -> Result<PublishAck, Error> {
    log::debug!("Publishing payment to NATS: {:?}", payment);

    // Publish the payment to the NATS stream
    context
        .publish("payments", payment)
        .await
        .expect("Failed to publish payment to NATS")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to acknowledge payment: {}", e))
}

pub async fn payment_summary(
    filters: PaymentSummaryFilters,
    pg_pool: Pool<sqlx::Postgres>,
) -> serde_json::Value {
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
        .fetch_all(&pg_pool)
        .await
        .expect("Failed to fetch payment summary");

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

    serde_json::json!({
        "default": default,
        "fallback": fallback
    })
}

pub async fn purge_payments(pg_pool: Pool<sqlx::Postgres>, context: Context) {
    let sql = "DELETE FROM payments";
    sqlx::query(sql)
        .execute(&pg_pool)
        .await
        .expect("Failed to purge payments");

    // purge payments from NATS
    let stream = context
        .get_stream("payments")
        .await
        .expect("Failed to get payments stream");

    stream
        .purge()
        .await
        .expect("Failed to purge payments from NATS");
}
