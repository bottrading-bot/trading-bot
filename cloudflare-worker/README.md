# Cloudflare Workers deployment

This is the scheduled Cloudflare version of the Alpaca bot. It uses the Alpaca REST API directly and does not keep a machine or local process running. A cron invocation runs every five minutes; Cloudflare's worker platform currently allows Python or JavaScript scheduled handlers, while this version uses TypeScript and `fetch` for the Alpaca API.

## Important behavior and safety

- `ENABLE_LIVE_ORDERS` defaults to `false`. The worker only sends real orders after the Cloudflare variable is deliberately changed to `true`.
- The worker always calls Alpaca's live API, never the paper endpoint. Keep live orders disabled until the account, credentials, and deployment logs have been checked.
- When enabled, the worker sends a whole-share Alpaca bracket order: a 3% stop-loss and a 5% take-profit. These broker-held exits remain attached when the scheduled worker is not running. Stop execution can still be worse than the stop price during gaps or fast markets.
- To keep the broker-held bracket compatible with a small account and a GTC stop, the worker only buys whole shares. If a candidate costs more than its risk-limited cash budget, it skips the trade instead of using a fractional entry with a software-only stop.
- Exit handling in this Cloudflare version is the fixed Alpaca bracket (3% stop / 5% take-profit). It does not reproduce the local Python runner's trailing-stop or momentum-reversal exits.
- It will not enter while an account position or open order exists, or if it sees an untracked position/state. It limits round trips to 3 in the most recent 5 Alpaca sessions and blocks new entries after a 5% daily equity loss.
- Before considering an existing bot position managed, it re-reads the parent order and checks that Alpaca reports an active stop leg. If not, it logs a critical error and blocks further entries; check the position and orders in Alpaca manually.
- Cloudflare Free limits scheduled invocations to 10 ms CPU time and 15 minutes wall time per cron run. The worker intentionally fails closed on missing data, pagination, API errors, or malformed state.

## Deploy from GitHub

1. In Cloudflare, create a Workers & Pages project and connect the GitHub repository. Set the build/deploy root to `cloudflare-worker` if the repository contains the whole project; otherwise select this folder as the project root.
2. Create a Workers KV namespace named `BOT_STATE`, then replace `REPLACE_WITH_KV_NAMESPACE_ID` in `wrangler.jsonc` with the namespace ID. Keep this namespace stable; deleting it resets the bot's local risk counters.
3. Add these as **secrets**, not plain variables: `ALPACA_API_KEY` and `ALPACA_SECRET_KEY`. Use new Alpaca live keys and never put them in GitHub.
4. Keep `ENABLE_LIVE_ORDERS=false` while checking the Cloudflare build and cron logs. The worker will print signals but will not submit orders.
5. Only after the safeguards and account setup are confirmed, change the Cloudflare variable `ENABLE_LIVE_ORDERS` to `true` and redeploy.

The scheduled handler checks Alpaca's clock, so a cron run outside the regular US session returns without trading. The five-minute cron runs in UTC, but it does not need a manually adjusted timezone because Alpaca's clock is the source of truth.

## Local development

Requires Node.js. Use `npm install`, then `npx wrangler dev --test-scheduled`. Put credentials only in `.dev.vars` (which is ignored by Git) and keep live orders disabled. Do not use an actual live order as a deployment check.

