// Copyright 2026 Salesforce, Inc. All rights reserved.
mod generated;

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;

use pdk::hl::*;
use pdk::logger;
use pdk::metadata::Metadata;

/// Request details captured in the request filter and handed to the response filter.
struct RequestInfo {
    started_ms: u128,
    consumer: String,
    partition: String,
    plan: String,
    amount_bucket: String,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// The single log entry that combines the request and the response.
/// Fields are serialized in declaration order, so keep this order as-is.
#[derive(Serialize)]
struct LogEntry<'a> {
    consumer: String,
    api_product: &'a str,
    partition: String,
    plan: String,
    status: u32,
    latency: u64,
    amount_bucket: String,
}

/// Renders the log line for one request/response pair.
fn build_entry(api_product: &str, req: RequestInfo, status: u32, finished_ms: u128) -> String {
    let entry = LogEntry {
        consumer: req.consumer,
        api_product,
        partition: req.partition,
        plan: req.plan,
        status,
        latency: finished_ms.saturating_sub(req.started_ms) as u64,
        amount_bucket: req.amount_bucket,
    };
    serde_json::to_string(&entry).unwrap_or_default()
}

async fn request_filter(request_state: RequestState) -> Flow<RequestInfo> {
    let headers_state = request_state.into_headers_state().await;
    let handler = headers_state.handler();
    let header = |name: &str| handler.header(name).unwrap_or_else(|| "unknown".to_string());

    // Pass the request details to the response phase instead of logging now.
    Flow::Continue(RequestInfo {
        started_ms: now_ms(),
        consumer: header("consumer"),
        partition: header("partition"),
        plan: header("plan"),
        amount_bucket: header("amount_bucket"),
    })
}

async fn response_filter(
    response_state: ResponseState,
    request_data: RequestData<RequestInfo>,
    api_product: &str,
) {
    let headers_state = response_state.into_headers_state().await;

    // Only log when the request filter ran and forwarded its data.
    let req = match request_data {
        RequestData::Continue(req) => req,
        _ => return,
    };

    let entry = build_entry(api_product, req, headers_state.status_code(), now_ms());
    logger::info!("{}", entry);
}

#[entrypoint]
async fn configure(launcher: Launcher, metadata: Metadata) -> Result<()> {
    let api_product = metadata
        .api_metadata
        .name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    let filter = on_request(request_filter)
        .on_response(|rs, request_data| response_filter(rs, request_data, &api_product));
    launcher.launch(filter).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use pdk_unit::{TraceBackend, UnitHttpMessage, UnitHttpRequest, UnitHttpResponse, UnitTestBuilder};
    use std::rc::Rc;

    fn info(started_ms: u128, amount_bucket: &str) -> RequestInfo {
        RequestInfo {
            started_ms,
            consumer: "HDBankWeb".to_string(),
            partition: "HDBank".to_string(),
            plan: "Gold".to_string(),
            amount_bucket: amount_bucket.to_string(),
        }
    }

    #[test]
    fn entry_has_exactly_the_requested_fields_in_order() {
        let entry = build_entry("hdb-03", info(1_000, "High"), 202, 1_042);

        assert_eq!(
            entry,
            r#"{"consumer":"HDBankWeb","api_product":"hdb-03","partition":"HDBank","plan":"Gold","status":202,"latency":42,"amount_bucket":"High"}"#
        );
    }

    #[test]
    fn latency_never_goes_negative() {
        let entry = build_entry("hdb-01", info(2_000, "N/A"), 200, 1_000);
        assert!(entry.contains(r#""latency":0,"#));
    }

    #[test]
    fn quotes_in_header_values_are_escaped() {
        let mut req = info(0, "High");
        req.consumer = r#"Web "beta""#.to_string();
        let entry = build_entry("hdb-04", req, 200, 0);
        assert!(entry.starts_with(r#"{"consumer":"Web \"beta\"","#));
    }

    fn backend_201(_req: UnitHttpRequest) -> UnitHttpResponse {
        UnitHttpResponse::new(201)
    }

    #[test]
    fn policy_passes_traffic_through_unchanged() {
        let backend = Rc::new(TraceBackend::new(backend_201));
        let mut tester = UnitTestBuilder::default()
            .with_config("{}".to_string())
            .with_backend(Rc::clone(&backend))
            .with_entrypoint(super::configure);

        let response = tester.request(UnitHttpRequest::get().with_header("plan", "Gold"));
        assert_eq!(response.status_code(), 201);

        // The policy only logs: the backend still receives the original header.
        let request = backend.next().unwrap();
        assert_eq!(request.header("plan"), Some("Gold"));
    }
}
