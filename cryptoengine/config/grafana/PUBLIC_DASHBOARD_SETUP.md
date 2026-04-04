# Public Dashboard Setup Guide

This guide explains how to enable the Grafana public dashboard link for the
**Public Performance Dashboard** so you can share trading results without
exposing any sensitive data.

---

## Prerequisites

`GF_FEATURE_TOGGLES_ENABLE: "publicDashboards"` is already set in
`docker-compose.yml` for the `grafana` service. No extra configuration is
needed — the feature is active as soon as Grafana starts.

---

## Step-by-step: enabling the public link

1. **Open Grafana** in your browser: http://localhost:3002

2. **Log in** with `admin / GrafanaAdmin2026!`

3. **Navigate to the dashboard**
   - Click the search icon (magnifying glass) in the left sidebar
   - Search for **"Public Performance Dashboard"**
   - Click the result to open it

4. **Open the Share dialog**
   - Click the **Share** icon in the top toolbar (it looks like a chain link /
     share icon, to the right of the dashboard title)
   - Alternatively: `Dashboard menu (...)` → **Share**

5. **Go to the "Public Dashboard" tab**
   - In the Share modal, click the **Public Dashboard** tab

6. **Enable public access**
   - Toggle **"Enable sharing"** to ON
   - Grafana generates a unique public URL — copy and save it

7. **Copy the URL**
   - The URL is something like:
     `http://localhost:3002/public-dashboards/<random-token>`
   - Anyone with this URL can view the dashboard **without logging in**

8. **Save** (Grafana saves automatically when you toggle the setting)

---

## What is safe to share

The Public Performance Dashboard is specifically designed to contain **no
sensitive data**. The following information IS shown:

| Panel | What is shown | Why it is safe |
|-------|---------------|----------------|
| Cumulative PnL % | Percentage gain/loss trend | No absolute USDT balance |
| Win Rate % | Ratio of winning trades | Aggregated statistic only |
| Total Trades | Trade count | No individual trade details |
| Avg Trade Duration | Hours held on average | Aggregated statistic only |
| Sharpe Ratio (est.) | Risk-adjusted return estimate | Derived metric only |
| Strategy Breakdown | PnL per strategy | Strategy names + aggregated PnL |
| Daily PnL bar chart | Day-by-day PnL amounts | Daily aggregates only |
| Funding Payments | Total funding income | Aggregated sum only |

## What is NOT shown (kept private)

- Current account balance or equity
- Open positions and their sizes
- API keys or credentials (never visible in Grafana)
- Individual trade entry/exit prices or sizes
- Internal system state (Redis channels, DB passwords)

Do **not** share links to the following dashboards publicly:

- `Live Performance` — shows current equity and position details
- `Strategy Monitor` — exposes strategy states and capital allocation
- `Market Regime` — shows internal signal data
- `LLM Advisor / Reports` — shows internal AI judgements
- `Backtest Results` — shows full strategy parameters

---

## Revoking public access

To disable the public link at any time:

1. Open the **Public Performance Dashboard**
2. Click **Share** → **Public Dashboard** tab
3. Toggle **"Enable sharing"** to OFF

The token becomes invalid immediately and the URL stops working.

---

## Using a reverse proxy for external access

If you want to share the dashboard over the internet (not just localhost):

1. Point a reverse proxy (nginx, Caddy, Traefik) at `localhost:3002`
2. Enable HTTPS on the proxy
3. Replace `localhost:3002` in the public URL with your domain
4. Consider rate-limiting the `/public-dashboards/` path

The public dashboard endpoint does not require authentication, so HTTPS and
rate-limiting are recommended before sharing externally.
