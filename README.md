# Portfolio Tracker

A Streamlit-based personal investment portfolio dashboard for tracking multi-broker, multi-currency holdings with real-time pricing.

## Features

- **Multi-Broker Support** — Track positions across any broker (e.g. 富途, 中信, Robinhood, Interactive Brokers)
- **Multi-Currency** — USD, HKD, JPY, CNY with live FX conversion (all values in CNY)
- **Multi-Market** — A股, B股, 港股, 美股, 日股, 基金
- **Real-Time Prices** — Yahoo Finance for stocks, 天天基金 for Chinese mutual funds
- **NAV Tracking** — Daily snapshots with cumulative NAV curve and benchmark comparison (CSI 300, S&P 500, Hang Seng)
- **Return Attribution** — Per-market and per-stock P&L contribution breakdown
- **Industry Analysis** — Sector allocation via yfinance (free) or FMP API
- **CSV Import** — Bulk import positions, cash, and closed trades via sidebar UI
- **P&L Journal** — Rolling 30-day net P&L history from daily snapshots
- **Capital Breakdown** — Detailed capital composition with flexible calculation modes

## Screenshot

> Add a screenshot of your dashboard here.

## Quick Start

```bash
# Clone
git clone https://github.com/alanhewenyu/portfolio-tracker.git
cd portfolio-tracker

# Install dependencies
pip install -r requirements.txt

# Configure environment (optional — works out-of-the-box with defaults)
cp .env.example .env

# Run
streamlit run dashboard.py
```

## Data Import

Positions can be managed via:

1. **Sidebar UI** — Add/edit/delete individual positions in the "Edit" tab
2. **CSV Import** — Bulk import via the "Import" tab in the sidebar. Download CSV templates from the tab, fill in your data, and upload.
3. **Excel Import** — CLI tool for specific broker statement format:
   ```bash
   export PORTFOLIO_EXCEL=~/Desktop/your_portfolio.xlsm
   python import_excel.py
   ```

## Configuration

Copy `.env.example` to `.env` to customize (all settings are optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `FUTU_CAPITAL` | `0` | Historical CNY deposits to a specific broker. Leave at 0 for cost-based capital. |
| `FUTU_DEPOSIT_FX` | `1.0` | Average USD/CNY rate at deposit time (only used if FUTU_CAPITAL > 0) |
| `B_SHARE_CAPITAL` | `0` | Historical CNY deposits for B-shares. Leave at 0 for cost-based capital. |
| `PORTFOLIO_DB_PATH` | `./portfolio.db` | Custom SQLite database path |
| `FMP_API_KEY` | _(empty)_ | FMP API key for industry data (optional — akshare + yfinance used as free fallback) |

### Capital Modes

The tracker supports two capital calculation modes, auto-detected from environment variables:

**Cost Mode** (default, recommended for new users):
- When `FUTU_CAPITAL` and `B_SHARE_CAPITAL` are both 0
- Capital = position cost + cash - off-exchange leverage - all realized P&L
- Works out-of-the-box with zero configuration

**Deposit Mode** (advanced):
- When either `FUTU_CAPITAL` or `B_SHARE_CAPITAL` is set to a non-zero value
- Uses hardcoded deposit amounts for specific brokers
- Enables detailed FX impact analysis and margin interest tracking

## Architecture

```
dashboard.py    — Main Streamlit app (KPI cards, charts, tables, sidebar CRUD)
db.py           — SQLite schema, migrations, capital calculation, CRUD operations
prices.py       — Price fetching (yfinance, 天天基金), FX rates, caching
snapshot.py     — Cron job for daily NAV snapshots
import_excel.py — Bulk import from Excel (broker statement format)
fmp.py          — Industry classification (FMP API / akshare / yfinance fallback)
```

## Daily Snapshots

Set up a cron job to capture daily NAV:

```bash
# Example: run at 6:00 AM daily
0 6 * * * cd /path/to/portfolio-tracker && python snapshot.py >> snapshot.log 2>&1
```

## Tech Stack

- **Frontend**: Streamlit + Plotly
- **Database**: SQLite (WAL mode)
- **Prices**: yfinance, 天天基金 API
- **FX**: Yahoo Finance with exchangerate.host fallback
- **Industry**: akshare (A/B股, 基金) + yfinance (US/HK/JP) with optional FMP API

## License

MIT License — see [LICENSE](LICENSE) for details.
