#![cfg(feature = "clob")]
#![allow(
    clippy::unwrap_used,
    reason = "Do not need additional syntax for setting up tests"
)]

//! A prepared submission transmits exactly the bytes `post_order` sends, and every resend of it
//! transmits them again, answered as received with no settlement-hash enrichment.

mod common;

use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use httpmock::Method::POST;
use httpmock::MockServer;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, TickSize};
use polymarket_client_sdk_v2::types::Decimal;
use reqwest::StatusCode;
use serde_json::json;

use crate::common::{PRIVATE_KEY, create_authenticated, token_1};

#[tokio::test]
async fn a_prepared_submission_sends_the_bytes_post_order_sends() -> anyhow::Result<()> {
    let server = MockServer::start();
    let client = create_authenticated(&server).await?;
    client.set_tick_size(token_1(), TickSize::Hundredth);
    client.set_neg_risk(token_1(), false);
    client.set_version(2);
    let signer = LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON));
    let signable = client
        .limit_order()
        .token_id(token_1())
        .price(Decimal::from_str("0.42")?)
        .size(Decimal::from(5))
        .side(Side::Buy)
        .order_type(OrderType::GTC)
        .post_only(true)
        .build()
        .await?;
    let signed = client.sign(&signer, signable).await?;
    let identity = signed.signing.unwrap();
    assert_eq!(identity.domain_version, "2");

    let prepared = client.prepare_post_order(&signed)?;
    let body = prepared.body().unwrap().to_vec();
    assert_eq!(body, serde_json::to_vec(&signed)?);

    // `post_order` (which never matched here, so it does not poll) sends exactly those bytes,
    // and so does each `send_prepared` of the same preparation.
    let answer = json!({
        "error_msg": "",
        "makingAmount": "",
        "orderID": format!("{}", identity.order_hash),
        "status": "live",
        "success": true,
        "takingAmount": "",
        "transactionsHashes": []
    });
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/order")
            .body(String::from_utf8(body.clone()).unwrap());
        then.status(StatusCode::OK).json_body(answer.clone());
    });
    client.post_order(signed).await?;
    let first = client.send_prepared(&prepared).await?;
    let second = client.send_prepared(&prepared).await?;
    mock.assert_calls(3);
    assert_eq!(first.status, 200);
    assert_eq!(first, second);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.body)?,
        answer
    );
    Ok(())
}
