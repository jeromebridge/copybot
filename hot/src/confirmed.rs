//! Dry-only observations of confirmed fills, not pending order reconstruction.
use crate::{calldata::Decoded, feeds::RawTx};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tokio::sync::mpsc;

pub fn validate(mode: &str, secs: u64, feeds: bool, txpool: bool) -> Result<(), String> {
    if mode != "dry" || !(10..=3600).contains(&secs) || feeds || txpool {
        return Err(
            "confirmed polling requires mode=dry, 10..3600 seconds, no feeds and no txpool_rpc"
                .into(),
        );
    }
    Ok(())
}

#[derive(Clone, Deserialize)]
struct Activity {
    proxy_wallet: String,
    timestamp: u64,
    transaction_hash: String,
    condition_id: String,
    token_id: String,
    side: String,
    size: f64,
    price: f64,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    is_combo: bool,
}
#[derive(Deserialize)]
struct Page {
    data: Vec<Activity>,
    pagination: Pagination,
}
#[derive(Deserialize)]
struct Pagination {
    has_more: bool,
    next_cursor: Option<String>,
}
#[derive(Default, Serialize, Deserialize)]
struct Checkpoint {
    since: u64,
    seen: BTreeMap<String, u64>,
}

fn decode(row: &Activity, wallet: [u8; 20]) -> Result<Option<RawTx>, String> {
    if row.kind != "TRADE" || row.is_combo {
        return Ok(None);
    }
    if crate::config::addr20(&row.proxy_wallet)? != wallet {
        return Err("wallet mismatch".into());
    }
    let side = match row.side.as_str() {
        "BUY" => 0,
        "SELL" => 1,
        _ => return Err("invalid side".into()),
    };
    if !row.size.is_finite()
        || row.size <= 0.0
        || !row.price.is_finite()
        || row.price <= 0.0
        || row.price >= 1.0
    {
        return Err("invalid fill amount or price".into());
    }
    if row.token_id.is_empty() || !row.token_id.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid token".into());
    }
    let condition: [u8; 32] = hex::decode(row.condition_id.trim_start_matches("0x"))
        .map_err(|_| "invalid condition")?
        .try_into()
        .map_err(|_| "invalid condition length")?;
    let hash = hex::decode(row.transaction_hash.trim_start_matches("0x"))
        .map_err(|_| "invalid transaction")?;
    if hash.len() != 32 {
        return Err("invalid transaction length".into());
    }
    // A public activity fill has no order ID, original order size or maker role.
    let id = format!(
        "{}:{}:{}:{}:{}:{}:{}",
        hex::encode(wallet),
        row.transaction_hash.to_lowercase(),
        row.token_id,
        side,
        row.timestamp,
        row.size,
        row.price
    );
    let salt = crate::order::keccak(id.as_bytes());
    Ok(Some(RawTx {
        confirmed: Some((
            wallet,
            Decoded {
                condition_id: condition,
                token_id: row.token_id.clone(),
                side,
                price: row.price,
                order_size: row.size,
                fill_size: row.size,
                role: "confirmed",
                salt,
                occurrence: 0,
            },
        )),
        source: "confirmed-activity".into(),
        hash: format!("{}:{}", row.transaction_hash, hex::encode(salt)),
        input: vec![],
        to: String::new(),
        to_neg_risk: false,
        seen_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    }))
}

pub struct Poller {
    leaders: Vec<(String, [u8; 20])>,
    path: PathBuf,
    state: BTreeMap<String, Checkpoint>,
}
impl Poller {
    pub fn open(leaders: Vec<(String, [u8; 20])>, path: String) -> Result<Self, String> {
        let path = PathBuf::from(path);
        let mut state: BTreeMap<String, Checkpoint> = match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| e.to_string())?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.to_string()),
        };
        let now = crate::ledger::now_secs().max(0) as u64;
        for (_, wallet) in &leaders {
            state.entry(hex::encode(wallet)).or_insert(Checkpoint {
                since: now,
                seen: BTreeMap::new(),
            });
        }
        let p = Self {
            leaders,
            path,
            state,
        };
        p.save()?;
        Ok(p)
    }
    fn save(&self) -> Result<(), String> {
        let tmp = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(&self.state).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
        std::fs::rename(tmp, &self.path).map_err(|e| e.to_string())
    }
    async fn fetch(
        client: &reqwest::Client,
        wallet: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<Activity>, String> {
        let mut rows = vec![];
        let mut cursor = String::new();
        for _ in 0..20 {
            let mut request = client
                .get("https://data-api.polymarket.com/v2/activity")
                .query(&[
                    ("user", wallet),
                    ("type", "TRADE"),
                    ("limit", "1000"),
                    ("sort_direction", "ASC"),
                ])
                .query(&[("start", start), ("end", end)]);
            if !cursor.is_empty() {
                request = request.query(&[("cursor", &cursor)]);
            }
            let page: Page = request
                .send()
                .await
                .map_err(|_| "activity connection failed")?
                .error_for_status()
                .map_err(|e| format!("activity HTTP {:?}", e.status()))?
                .json()
                .await
                .map_err(|_| "invalid activity response")?;
            rows.extend(page.data);
            if !page.pagination.has_more {
                return Ok(rows);
            }
            let next = page
                .pagination
                .next_cursor
                .ok_or("missing activity cursor")?;
            if next.is_empty() || next == cursor {
                return Err("repeated activity cursor".into());
            }
            cursor = next;
        }
        Err("activity page limit reached; checkpoint retained".into())
    }
    pub async fn run(
        mut self,
        tx: mpsc::UnboundedSender<RawTx>,
        secs: u64,
        wake: std::sync::Arc<tokio::sync::Notify>,
    ) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        loop {
            for (name, wallet) in self.leaders.clone() {
                let key = hex::encode(wallet);
                let now = crate::ledger::now_secs().max(0) as u64;
                let start = self.state[&key].since;
                match Self::fetch(&client, &format!("0x{key}"), start, now).await {
                    Ok(rows) => {
                        let mut observations = vec![];
                        let mut invalid = 0;
                        for row in &rows {
                            if row.timestamp < start || row.timestamp > now {
                                invalid += 1;
                                continue;
                            }
                            match decode(row, wallet) {
                                Ok(Some(raw)) => observations.push((raw, row.timestamp)),
                                Ok(None) => {}
                                Err(_) => invalid += 1,
                            }
                        }
                        let cp = self.state.get_mut(&key).unwrap();
                        let mut fresh = vec![];
                        for (raw, timestamp) in observations {
                            if !cp.seen.contains_key(&raw.hash) {
                                cp.seen.insert(raw.hash.clone(), timestamp);
                                fresh.push(raw);
                            }
                        }
                        cp.since = now.saturating_sub(secs.max(120) * 2).max(start);
                        cp.seen.retain(|_, timestamp| *timestamp >= cp.since);
                        // Persist before delivery: a crash can miss a dry observation, never replay it.
                        if let Err(e) = self.save() {
                            eprintln!("confirmed checkpoint failed: {e}; poller stopped");
                            return;
                        }
                        println!(
                            "{}",
                            serde_json::json!({"ev":"confirmed_poll","lane":name,"rows":rows.len(),"new_fills":fresh.len(),"invalid":invalid,"t":now*1000})
                        );
                        for raw in fresh {
                            if tx.send(raw).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => eprintln!(
                        "{}",
                        serde_json::json!({"ev":"confirmed_poll_error","lane":name,"error":e,"t":now*1000})
                    ),
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(secs)) => {},
                _ = wake.notified() => {
                    // Coalesce bursts and give the public activity index time to catch up.
                    tokio::time::sleep(Duration::from_secs(3)).await;
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row() -> Activity {
        Activity {
            proxy_wallet: format!("0x{}", "11".repeat(20)),
            timestamp: 123,
            transaction_hash: format!("0x{}", "22".repeat(32)),
            condition_id: format!("0x{}", "33".repeat(32)),
            token_id: "123".into(),
            side: "BUY".into(),
            size: 10.0,
            price: 0.5,
            kind: "TRADE".into(),
            is_combo: false,
        }
    }
    #[test]
    fn live_and_mixed_feeds_are_rejected() {
        assert!(validate("dry", 30, false, false).is_ok());
        for m in ["live", "shadow", "paper"] {
            assert!(validate(m, 30, false, false).is_err());
        }
        assert!(validate("dry", 30, true, false).is_err());
        assert!(validate("dry", 30, false, true).is_err());
        assert!(validate("dry", 1, false, false).is_err());
    }
    #[test]
    fn confirmed_identity_distinguishes_fills_and_wallets() {
        let a = decode(&row(), [0x11; 20]).unwrap().unwrap();
        assert_eq!(a.hash, decode(&row(), [0x11; 20]).unwrap().unwrap().hash);
        let mut r = row();
        r.size = 20.0;
        assert_ne!(a.hash, decode(&r, [0x11; 20]).unwrap().unwrap().hash);
        assert!(decode(&r, [0x12; 20]).is_err());
        r.price = f64::NAN;
        assert!(decode(&r, [0x11; 20]).is_err());
        r = row();
        r.is_combo = true;
        assert!(decode(&r, [0x11; 20]).unwrap().is_none());
    }
    #[test]
    fn checkpoints_survive_restart_and_corruption_fails_closed() {
        let path = std::env::temp_dir().join(format!(
            "confirmed-test-{}-{}.json",
            std::process::id(),
            crate::order::safe_salt(123)
        ));
        let leaders = vec![("test".to_string(), [0x11; 20])];
        let mut p = Poller::open(leaders.clone(), path.to_string_lossy().into()).unwrap();
        let key = hex::encode([0x11; 20]);
        let raw = decode(&row(), [0x11; 20]).unwrap().unwrap();
        p.state
            .get_mut(&key)
            .unwrap()
            .seen
            .insert(raw.hash.clone(), 123);
        p.save().unwrap();
        let restored = Poller::open(leaders.clone(), path.to_string_lossy().into()).unwrap();
        assert!(restored.state[&key].seen.contains_key(&raw.hash));
        std::fs::write(&path, b"broken").unwrap();
        assert!(Poller::open(leaders, path.to_string_lossy().into()).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
