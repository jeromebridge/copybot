//! Wallet-filtered mined-fill notifications. Activity API remains the decoder.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

fn filters(wallets: &[[u8; 20]]) -> Vec<Value> {
    if wallets.is_empty() {
        return vec![];
    }
    let topics: Vec<String> = wallets
        .iter()
        .map(|w| format!("0x{}{}", "0".repeat(24), hex::encode(w)))
        .collect();
    let event = format!("0x{}", hex::encode(crate::order::keccak(b"OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")));
    let addresses = json!([
        format!("0x{}", crate::feeds::CTF_EXCHANGE_V2),
        format!("0x{}", crate::feeds::NEG_RISK_CTF_EXCHANGE_V2)
    ]);
    // Separate subscriptions implement maker OR taker, never an unfiltered feed.
    vec![
        json!({"address": addresses, "topics": [event, null, topics]}),
        json!({"address": addresses, "topics": [event, null, null, topics]}),
    ]
}

fn matches(log: &Value, filters: &[Value]) -> bool {
    filters.iter().any(|filter| {
        let Some(address) = log["address"].as_str() else {
            return false;
        };
        if !filter["address"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a.as_str().unwrap().eq_ignore_ascii_case(address))
        {
            return false;
        }
        let Some(actual) = log["topics"].as_array() else {
            return false;
        };
        actual.len() == 4
            && filter["topics"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .all(|(i, expected)| {
                    if expected.is_null() {
                        return true;
                    }
                    let Some(value) = actual[i].as_str() else {
                        return false;
                    };
                    if let Some(options) = expected.as_array() {
                        options
                            .iter()
                            .any(|v| v.as_str().unwrap().eq_ignore_ascii_case(value))
                    } else {
                        expected.as_str().unwrap().eq_ignore_ascii_case(value)
                    }
                })
    })
}

async fn session(url: &str, filters: &[Value], wake: &Notify) -> Result<(), &'static str> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .map_err(|_| "connect timeout")?
    .map_err(|_| "connect failed")?;
    let mut subscriptions = Vec::new();
    for (i, filter) in filters.iter().enumerate() {
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0", "id":i+1,"method":"eth_subscribe","params":["logs",filter]})
                .to_string(),
        ))
        .await
        .map_err(|_| "subscribe send failed")?;
        let ack = async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value =
                            serde_json::from_str(&text).map_err(|_| "invalid response")?;
                        if v["id"].as_u64() == Some((i + 1) as u64) {
                            return v["result"]
                                .as_str()
                                .map(str::to_owned)
                                .ok_or("subscription rejected");
                        }
                        if v["method"] == "eth_subscription" {
                            wake.notify_one();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        ws.send(Message::Pong(p)).await.map_err(|_| "pong failed")?
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    _ => return Err("closed before acknowledgment"),
                }
            }
        };
        subscriptions.push(
            tokio::time::timeout(Duration::from_secs(15), ack)
                .await
                .map_err(|_| "ack timeout")??,
        );
    }
    println!(
        "{}",
        json!({"ev":"confirmed_ws_connected","subscriptions":subscriptions.len()})
    );
    wake.notify_one(); // Catch up through the persisted activity checkpoint after reconnect.
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    let mut last_received = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if last_received.elapsed() > Duration::from_secs(90) { return Err("heartbeat timeout"); }
                tokio::time::timeout(Duration::from_secs(10), ws.send(Message::Ping(vec![]))).await
                    .map_err(|_| "ping timeout")?.map_err(|_| "ping failed")?;
            },
            message = ws.next() => {
                last_received = tokio::time::Instant::now();
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).map_err(|_| "invalid notification")?;
                        if v["method"] != "eth_subscription" { continue; }
                        if !subscriptions.iter().any(|s| v["params"]["subscription"].as_str() == Some(s.as_str())) { continue; }
                        let log = &v["params"]["result"];
                        if !matches(log, filters) { return Err("notification outside wallet filter"); }
                        println!("{}", json!({"ev":"confirmed_ws_event","bytes":text.len(),"removed":log["removed"]}));
                        wake.notify_one();
                    },
                    Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.map_err(|_| "pong failed")?,
                    Some(Ok(Message::Pong(_))) => {},
                    _ => return Err("connection closed"),
                }
            }
        }
    }
}

pub async fn run(url: String, wallets: Vec<[u8; 20]>, wake: Arc<Notify>) {
    let filters = filters(&wallets);
    if filters.is_empty() {
        return;
    }
    let mut delay = 5;
    loop {
        let start = tokio::time::Instant::now();
        let result = session(&url, &filters, &wake).await;
        // Never log provider errors or URLs: either can contain the API key.
        eprintln!(
            "{}",
            json!({"ev":"confirmed_ws_disconnected","reason":result.err(),"retry_secs":delay})
        );
        tokio::time::sleep(Duration::from_secs(delay)).await;
        delay = if start.elapsed() > Duration::from_secs(120) {
            5
        } else {
            (delay * 2).min(300)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn acknowledged_subscriptions_notify_and_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let f = filters(&[[0x11; 20]]);
        let expected = f.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            for (i, filter) in expected.iter().enumerate() {
                let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let request: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(request["method"], "eth_subscribe");
                assert_eq!(request["params"], json!(["logs", filter]));
                ws.send(Message::Text(
                    json!({"id":i+1,"result":format!("sub{i}")}).to_string(),
                ))
                .await
                .unwrap();
            }
            ws.send(Message::Text(json!({"method":"eth_subscription","params":{
                "subscription":"sub0", "result":{"address":expected[0]["address"][0],
                "topics":[expected[0]["topics"][0],"0xorder",expected[0]["topics"][2][0],"0xother"],"removed":false}
            }}).to_string())).await.unwrap();
            ws.close(None).await.unwrap();
        });
        let wake = Notify::new();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            session(&format!("ws://{address}"), &f, &wake),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .unwrap();
        server.await.unwrap();
    }
    #[test]
    fn filters_are_wallet_scoped_on_both_roles() {
        assert!(filters(&[]).is_empty());
        let f = filters(&[[0x11; 20], [0x22; 20]]);
        assert_eq!(f.len(), 2);
        for role in [2, 3] {
            let mut topics = vec![
                f[0]["topics"][0].clone(),
                json!("0xorder"),
                json!("0xother"),
                json!("0xother"),
            ];
            topics[role] = f[0]["topics"][2][0].clone();
            let mut log = json!({"address":f[0]["address"][0],"topics":topics});
            assert!(matches(&log, &f));
            log["address"] = json!("0xunrelated");
            assert!(!matches(&log, &f));
            log["address"] = f[0]["address"][0].clone();
            log["topics"][role] = json!("0xother");
            assert!(!matches(&log, &f));
        }
    }
}
