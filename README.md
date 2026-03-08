# Investment Portfolio Tracker

A Streamlit-based personal investment portfolio dashboard for tracking multi-broker, multi-currency holdings with real-time pricing.

## Features

- **Multi-Broker Support** — Track positions across brokers (富途 / 中信 / 招商 / 招商永隆 / 支付宝)
- **Multi-Currency** — USD, HKD, JPY, CNY with live FX conversion (all values in CNY)
- **Multi-Market** — A股, B股, 港股, 美股, 日股, 基金
- **Real-Time Prices** — Yahoo Finance for stocks, 天天基金 for Chinese mutual funds
- **NAV Tracking** — Daily snapshots with cumulative NAV curve and benchmark comparison (CSI 300, S&P 500, Hang Seng)
- **Return Attribution** — Per-market and per-stock P&L contribution breakdown
- **Industry Analysis** — Sector allocation via FMP API
- **P&L Journal** — Rolling 30-day net P&L history from daily snapshots
- **Capital Breakdown** — Detailed capital composition with FX impact analysis

## Screenshot

> Add a screenshot of your dashboard here.

## Quick Start

```bash
# Clone
git clone https://github.com/your-username/investment-portfolio.git
cd investment-portfolio

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your capital constants

# Initialize database (auto-created on first run)
streamlit run dashboard.py
```

## Configuration

Copy `.env.example` to `.env` and set your values:

| Variable | Description |
|----------|-------------|
| `FUTU_CAPITAL` | Total historical CNY deposits to Futu (富途) |
| `FUTU_DEPOSIT_FX` | Average USD/CNY exchange rate at deposit time |
| `B_SHARE_CAPITAL` | Total historical CNY deposits for B-shares |
| `PORTFOLIO_DB_PATH` | Custom SQLite database path (optional) |
| `FMP_API_KEY` | Financial Modeling Prep API key for industry data (optional) |

## Architecture

```
dashboard.py    — Main Streamlit app (KPI cards, charts, tables, sidebar CRUD)
db.py           — SQLite schema, migrations, constants, CRUD operations
prices.py       — Price fetching (yfinance, 天天基金), FX rates, caching
snapshot.py     — Cron job for daily NAV snapshots
import_excel.py — Bulk import from Excel (broker statement format)
fmp.py          — Industry classification via FMP API
```

## Data Import

Positions can be managed via the sidebar UI or bulk-imported from Excel:

```bash
# Set path to your Excel file
export PORTFOLIO_EXCEL=~/Desktop/investment_2026.xlsm
python import_excel.py
```

## Daily Snapshots

Set up a cron job to capture daily NAV:

```bash
# Example: run at 6:00 AM daily
0 6 * * * cd /path/to/investment-portfolio && python snapshot.py >> snapshot.log 2>&1
```

## Tech Stack

- **Frontend**: Streamlit + Plotly
- **Database**: SQLite (WAL mode)
- **Prices**: yfinance, 天天基金 API
- **FX**: Yahoo Finance with exchangerate.host fallback
- **Industry**: Financial Modeling Prep API

## License

MIT License — see [LICENSE](LICENSE) for details.
