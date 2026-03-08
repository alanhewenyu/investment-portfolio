# Investment Portfolio Dashboard — TODO

## Project Overview
Streamlit-based personal investment portfolio tracker.
- Multi-broker: 富途, 中信, 招商, etc.
- Multi-currency: USD, HKD, JPY, CNY (all converted to CNY)
- Real-time prices via Yahoo Finance + 天天基金(funds)
- SQLite database, daily snapshots via cron

### Key Files
- `dashboard.py` — Main Streamlit app (~2300 lines)
- `db.py` — Schema, migrations, constants (loaded from `.env`)
- `prices.py` — Price fetching, FX rates, caching
- `snapshot.py` — Cron job for daily NAV snapshots
- `fmp.py` — Industry classification via FMP API

---

## Roadmap

### 1. Engineering Quality

#### 1.1 Testing
- [ ] Unit tests for `compute_capital()` (multi-currency, multi-broker)
- [ ] Unit tests for `build_portfolio()` P&L calculations (given fixed price/fx → verify pnl_cny, daily_pnl, ytd_pnl)
- [ ] Snapshot record & restore regression tests

#### 1.2 Reliability
- [ ] Yahoo Finance retry with backoff (2 retries, 1s delay) — `tenacity` or simple loop
- [ ] A-share price source: replace yfinance with domestic API (东方财富/新浪) for .SS/.SZ tickers — lower latency, more reliable
- [ ] FX fallback source: add backup (e.g. exchangerate.host) when Yahoo FX fails
- [ ] Module-level `ThreadPoolExecutor` reuse (avoid create/destroy per call)

#### 1.3 Architecture
- [ ] Split `render_kpi()` (~350 lines) into `compute_pnl()` + `render_kpi_cards()` + `render_pnl_strips()`
- [ ] Extract price-fetching side effects from `build_portfolio()` (decouple from `@st.cache_data`)
- [ ] Unified `logging` module (replace `print()` + `pass` throughout)
- [ ] `prefetch_all()` — run FX and prices in parallel (save ~3-5s)
- [ ] Type hints for core functions

### 2. Product Features

#### 2.1 Net Asset Value Chart ✅
- [x] Plot cumulative NAV curve from `daily_snapshots` table
- [x] Compare against benchmarks (CSI 300, S&P 500, Hang Seng)
- [ ] Show alpha / excess return

#### 2.2 Return Attribution ✅
- [x] Per-market YTD contribution breakdown (哪个市场赚最多)
- [x] Per-stock contribution ranking (哪只股票贡献最大)
- [ ] Per-currency FX impact attribution

#### 2.3 Risk Analytics
- [ ] Volatility (annualized, from daily snapshots)
- [ ] Maximum drawdown (peak-to-trough)
- [ ] Sharpe ratio
- [ ] Position concentration alerts (single stock > threshold)
- [ ] Correlation heatmap between holdings

#### 2.4 Dividend Tracking
- [ ] New `dividends` table: date, ticker, amount, currency, tax_withheld
- [ ] Include dividends in total return calculation
- [ ] Dividend calendar view

#### 2.5 Trade Management
- [ ] Partial position close (sell portion, not all-or-nothing)
- [ ] Transaction cost tracking (commissions, stamp duty, margin interest per trade)
- [ ] Broker statement CSV import (富途/中信/招商 formats)

#### 2.6 Visualization
- [ ] Position timeline (buy/add/reduce/close events)
- [ ] Monthly/annual P&L heatmap (beyond current calendar)
- [ ] Sector allocation trend over time

#### 2.7 Alerts & Notifications
- [ ] Target price alerts (WeChat / Telegram push)
- [ ] Abnormal volatility alerts (single-day drop > threshold)
- [ ] Rebalance reminders (deviation from target allocation)

#### 2.8 Deployment & Mobile
- [x] Auto-backup SQLite to iCloud (~/Documents, via snapshot.py cron)
- [ ] Mobile-responsive CSS refinement for KPI cards & table
- [ ] Deploy to VPS + Cloudflare Tunnel for remote access

#### 2.9 Export
- [ ] Excel/PDF monthly/annual investment report with charts
- [ ] CSV export of holdings, closed trades, snapshots

---

## Priority Matrix

| Priority | Item | Rationale |
|----------|------|-----------|
| ~~P0~~ | ~~NAV curve + benchmark comparison~~ | ✅ Done |
| ~~P0~~ | ~~Return attribution (by market/stock)~~ | ✅ Done |
| P0 | Yahoo retry + A-share domestic API | Improves daily reliability |
| P1 | Dividend tracking | Essential for HK/US long-term holdings |
| P1 | Core logic unit tests | Safety net for future changes |
| P1 | `render_kpi()` refactor | Unblocks testability |
| P2 | Risk analytics (drawdown, Sharpe) | Nice-to-have for portfolio assessment |
| P2 | Partial close + trade costs | Better trade management |
| P3 | Deployment + mobile | Quality of life |
| P3 | Alerts & notifications | Convenience |
