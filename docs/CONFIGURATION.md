# Configuration

The engine retains its existing TOML configuration format; runtime operator and registry state remain JSON. Packaging has not introduced a new configuration parser or strategy model.

## Configuration layers

| Layer | Location | Purpose |
| --- | --- | --- |
| Boot configuration | `deploy/copybot2.toml` | Custody, feeds, initial lanes, sizing, execution |
| Process environment | `deploy/copybot.env` | Signing key, observer configuration, feature gates |
| Operator state | Under configured `control_path` | Arm/off/halt intent and related state |
| Accounting and recovery | Configured ledger and `data/`, `run/` | Durable execution and reconciliation evidence |

Only examples ship. Do not commit any populated layer. The bot does not load `.env` files automatically; the service templates use systemd's `EnvironmentFile` mechanism.

## Sizing

`mode = "pct"` uses a fractional copy percentage: `0.005` means 0.5%, not 5%. `shares` and `usd` are also accepted by the existing parser. Percentage copying is subject to minimum order sizes, budgets, execution prices, and the configured exit policy; it is not a guarantee of an exact share ratio.

Use either `bankroll_usd` with derived budget fractions or the parser's absolute-cap mode. Do not mix derived and absolute caps. Set measured `leader_max_order_usd` and `leader_peak_exposure_usd` for your selected leader. The engine validates these rather than borrowing another leader's statistics.

`compound` controls the existing realized-gain/loss sizing behavior. Set it intentionally. A lane budget is a strategy allocation inside a shared account, not security isolation between different owners.

## Execution

For a lower-bandwidth dry observer, set `bot.confirmed_poll_secs = 30` and
remove all `[[feed]]` entries and `bot.txpool_rpc`. This input is restricted
to `mode = "dry"` and polls only enabled leader wallets through Polymarket's
public activity API; it makes no Alchemy requests. Intervals from 10 to 3600
seconds are accepted. `confirmed_poll` log events report rows, new fills and
invalid records; `confirmed_poll_error` reports failed polls.

For wallet-filtered push notifications, also set `bot.confirmed_ws = true`
and supply a Polygon `wss://` endpoint in the `CONFIRMED_WSS_URL` environment
variable. Set `confirmed_poll_secs = 300` for a five-minute safety check.
This remains dry-only and cannot be combined with pending feeds or txpool.
Two `eth_subscribe logs` subscriptions filter V2 exchange `OrderFilled`
events by the configured wallets in maker and taker topics at the provider.
The endpoint is not stored in the TOML configuration or logged.

Notifications trigger a coalesced activity-API lookup after three seconds;
they do not decode or execute blockchain logs directly. Detection therefore
still depends on public API indexing. The periodic check and a checkpoint
overlap cover delayed indexing; reconnects trigger another catch-up lookup.
This is not a guarantee against arbitrarily late API records or chain reorgs.
Wallet changes require a restart. There is no broad-feed fallback.
`confirmed_ws_connected`, `confirmed_ws_event` and
`confirmed_ws_disconnected` log connection health and notification sizes.
Alchemy usage is nonzero: measure a short run before projecting monthly cost.
The local `deploy/start-dry.local.py` launcher uses this hybrid mode and reads
`ALCHEMY_WSS_URL` from `deploy/dry-run.local.env` without loading signing keys.

The first run starts at the current time. A checkpoint beside `control_path`
retains progress across restarts, with an overlap of twice the greater of
120 seconds and the configured polling interval for delayed records.
Failed or incomplete pagination does not advance progress. Keep that checkpoint
with the other state files. Delivery is at-most-once across a crash: a crash
after checkpointing but before processing can lose a dry observation.

Confirmed fills lack original order IDs, full order size and maker/taker role.
The observer treats each distinct reported fill as its own sizing input, uses
fill size as the size bound, and labels the source `confirmed-activity`.
Identical rows in one transaction collapse to one observation because the API
does not expose a unique fill index. Combo trades are excluded. Records arriving
beyond that overlap may be missed. This is for observing decisions, not
accurate execution timing, historical replay, or simulated portfolio returns.

The parser accepts `taker` or `hybrid`. Split buy/sell slippage fields take precedence over the legacy combined field. `copy_makers` and `copy_maker_sells` are separate choices. Preserve them when transferring a lane between boot configuration and the runtime registry.

Do not infer units from the historical `_c` suffix alone: inspect `hot/src/config.rs` and `hot/src/lanes.rs` for the value's actual use. This export preserves those calculations.

`sell_all_frac = 0.0` requests the current any-sell flatten policy. A nonzero threshold changes when an exit becomes a full flatten. `sell_floor_frac` constrains the exit price relative to the leader; allowing broad slippage can fill at a substantially worse price. Setting it to zero does not guarantee a full fill in an empty market.

## Observers

All services need the same install directory and engine port. The legacy observer wallet environment variables must be supplied for your instance. Runtime lane discovery and shared-wallet checks still use the engine's API; these observers are not interchangeable with a multi-tenant permissions system.

Notification credentials and recipients are optional and empty in the template. `GUARDIAN_ENFORCE`, `MERGE_RECONCILE`, `COPYBOT_MERGE_ENABLE`, and `ORPHAN_SWEEP` retain their existing meanings. The example leaves optional live-action gates off; enabling an on-chain feature requires the corresponding custody/RPC setup. Do not set optional gates simply to clear a warning.
