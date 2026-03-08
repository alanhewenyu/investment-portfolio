# Portfolio Tracker

[中文](README_CN.md) | English

A Streamlit-based investment portfolio dashboard for tracking multi-broker, multi-currency holdings with real-time pricing, risk analytics, and automated backups.

![Dashboard Screenshot](docs/screenshot.png)

## Features

- **Multi-Broker** — Track positions across any number of brokers (e.g. Futu, CITIC, Robinhood, Interactive Brokers)
- **Multi-Currency** — USD, HKD, JPY, CNY with live FX conversion; all values normalized to CNY
- **Multi-Market** — A-shares, B-shares, HK, US, Japan, mutual funds
- **Real-Time Prices** — Yahoo Finance for stocks, Tiantian Fund for Chinese mutual funds
- **NAV Tracking** — Daily snapshots with cumulative NAV curve and benchmark comparison (CSI 300, S&P 500, Hang Seng)
- **Risk Analytics** — Flow-adjusted volatility, Sharpe ratio, max drawdown, win rate, Calmar ratio with rolling trend chart
- **Return Attribution** — Per-market and per-stock P&L contribution breakdown
- **Industry Analysis** — Sector allocation via akshare + yfinance (free) or FMP API (optional)
- **Automated Backup** — SQLite backup API with configurable directory, 7-day daily + monthly retention
- **CSV Import** — Bulk import positions, cash, and closed trades via sidebar UI
- **P&L Journal** — Rolling 30-day net P&L history from daily snapshots
- **Capital Breakdown** — Detailed capital composition with flexible calculation modes

## Quick Start

```bash
git clone https://github.com/alanhewenyu/portfolio-tracker.git
cd portfolio-tracker
pip install -r requirements.txt

# Optional: customize settings
cp .env.example .env

streamlit run dashboard.py
```

## Data Import

1. **Sidebar UI** — Add/edit/delete individual positions in the "Edit" tab
2. **CSV Import** — Bulk import via the "Import" tab. Download templates, fill in your data, upload.
3. **Excel Import** — CLI tool for broker statement format:
   ```bash
   export PORTFOLIO_EXCEL=~/Desktop/your_portfolio.xlsm
   python import_excel.py
   ```

## Configuration

Copy `.env.example` to `.env` (all settings are optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `FUTU_CAPITAL` | `0` | Broker deposit amount (CNY). Leave at 0 for cost-based capital. |
| `FUTU_DEPOSIT_FX` | `1.0` | Average USD/CNY rate at deposit time |
| `B_SHARE_CAPITAL` | `0` | B-share deposit amount (CNY). Leave at 0 for cost-based capital. |
| `PORTFOLIO_DB_PATH` | `./portfolio.db` | Custom SQLite database path |
| `BACKUP_DIR` | `./backups` | Backup directory. Set to a cloud-synced folder for off-site backup. |
| `FMP_API_KEY` | _(empty)_ | FMP API key for industry data (optional — free fallback available) |

### Capital Modes

Auto-detected from environment variables:

- **Cost Mode** (default) — Capital = position cost + cash - leverage - realized P&L. Zero configuration needed.
- **Deposit Mode** — Set `FUTU_CAPITAL` or `B_SHARE_CAPITAL` > 0 for deposit-based tracking with FX impact analysis.

## Daily Snapshots & Backup

Set up a cron job to capture daily NAV and backup the database:

```bash
0 6 * * * cd /path/to/portfolio-tracker && python snapshot.py >> snapshot.log 2>&1
```

Backups are saved to `BACKUP_DIR` (default: `./backups/`). Retention: 7 daily + monthly archives (1st of each month, kept indefinitely).

**Tip:** Set `BACKUP_DIR` to a cloud-synced folder (e.g. iCloud, Dropbox, Google Drive) for automatic off-site backup.

## Architecture

```
dashboard.py    — Streamlit app (KPI, charts, tables, sidebar CRUD)
db.py           — SQLite schema, migrations, capital calculation, CRUD
prices.py       — Price fetching (yfinance, Tiantian Fund), FX rates, caching
snapshot.py     — Daily NAV snapshots + database backup
import_excel.py — Bulk import from Excel (broker statement format)
fmp.py          — Industry classification (FMP / akshare / yfinance fallback)
```

## Tech Stack

- **Frontend**: Streamlit + Plotly
- **Database**: SQLite (WAL mode)
- **Prices**: yfinance, Tiantian Fund API
- **FX**: Yahoo Finance + exchangerate.host fallback
- **Industry**: akshare + yfinance (free), optional FMP API

## Contributing

Contributions are welcome! Feel free to [open an issue](https://github.com/alanhewenyu/portfolio-tracker/issues) or submit a pull request. Contact: [alanhe@icloud.com](mailto:alanhe@icloud.com)

Scan to follow on WeChat:

<img src="https://jianshan.co/images/wechat-qrcode.jpg" alt="见山笔记 WeChat QR Code" width="200">

## License

MIT License — see [LICENSE](LICENSE) for details.
