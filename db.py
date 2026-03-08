"""Portfolio database schema and CRUD operations."""

import sqlite3
import os
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_PATH = os.environ.get(
    'PORTFOLIO_DB_PATH',
    os.path.join(os.path.dirname(__file__), 'portfolio.db'),
)

SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS positions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    name        TEXT NOT NULL,
    market      TEXT NOT NULL,           -- A股/B股/港股/美股/日股/基金
    broker      TEXT NOT NULL,           -- 中信证券/招商证券/富途/招商永隆/支付宝
    currency    TEXT NOT NULL,           -- CNY/USD/HKD/JPY
    quantity    REAL NOT NULL DEFAULT 0,
    cost_price  REAL NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'open',  -- open/closed
    updated_at  TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS closed_trades (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT,
    name         TEXT NOT NULL,
    market       TEXT NOT NULL,
    broker       TEXT NOT NULL,
    currency     TEXT NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    close_date   TEXT,
    notes        TEXT,
    quantity     REAL,
    cost_price   REAL,
    close_price  REAL,
    cost_total   REAL
);

CREATE TABLE IF NOT EXISTS cash_balances (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    account    TEXT NOT NULL,
    currency   TEXT NOT NULL,
    balance    REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(account, currency)
);

CREATE TABLE IF NOT EXISTS nav_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL UNIQUE,
    net_asset_value  REAL,       -- 净资产市值 (RMB)
    capital_invested REAL,       -- 自有资金投入 (RMB)
    pnl              REAL,       -- 盈亏
    equity_nav       REAL,       -- 权益资产净值
    benchmark_value  REAL        -- 沪深300
);

CREATE TABLE IF NOT EXISTS fx_rates (
    currency   TEXT PRIMARY KEY,  -- USD/HKD/JPY
    rate_to_cny REAL NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS margin_balances (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    broker     TEXT NOT NULL,
    category   TEXT NOT NULL,           -- 'in_house' (场内杠杆) / 'off_exchange' (场外杠杆)
    currency   TEXT NOT NULL DEFAULT 'CNY',
    amount     REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(broker, category, currency)
);

CREATE TABLE IF NOT EXISTS industry_cache (
    ticker     TEXT PRIMARY KEY,
    sector     TEXT,
    industry   TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS daily_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT NOT NULL UNIQUE,
    total_assets  REAL,         -- equity + cash (excluding leverage)
    net_assets    REAL,         -- equity + cash - leverage
    equity_mv_cny REAL,
    cash_cny      REAL,
    leverage_cny  REAL,
    total_pnl_cny REAL,
    market_data   TEXT,         -- JSON (legacy, kept for backward compat)
    created_at    TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS snapshot_market_detail (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    date     TEXT NOT NULL,
    market   TEXT NOT NULL,          -- A股/B股/港股/美股/日股/基金
    currency TEXT NOT NULL,          -- original trading currency
    mv       REAL NOT NULL DEFAULT 0, -- market value in local currency
    UNIQUE(date, market, currency)
);

CREATE TABLE IF NOT EXISTS ytd_baseline_prices (
    year       INTEGER NOT NULL,
    ticker     TEXT NOT NULL,
    price      REAL NOT NULL,
    currency   TEXT NOT NULL,
    date       TEXT NOT NULL,
    quantity   REAL,
    cost_price REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE(year, ticker)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_ticker_broker ON positions(ticker, broker);
CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(market);
CREATE INDEX IF NOT EXISTS idx_positions_broker ON positions(broker);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_closed_market ON closed_trades(market);
-- idx_nav_date removed: redundant with UNIQUE constraint on nav_history(date)
-- idx_snapshots_date removed: redundant with UNIQUE constraint on daily_snapshots(date)
CREATE INDEX IF NOT EXISTS idx_smd_date ON snapshot_market_detail(date);
CREATE INDEX IF NOT EXISTS idx_ytd_year ON ytd_baseline_prices(year);
"""


def get_conn(db_path=None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_closed_trades(conn):
    """Add quantity/cost/price/pnl_cny columns to closed_trades (idempotent)."""
    for col, col_type in [
        ('quantity', 'REAL'),
        ('cost_price', 'REAL'),
        ('close_price', 'REAL'),
        ('cost_total', 'REAL'),
        ('realized_pnl_cny', 'REAL'),
    ]:
        try:
            conn.execute(f"ALTER TABLE closed_trades ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # column already exists


def _migrate_margin_multi_currency(conn):
    """Migrate margin_balances UNIQUE from (broker, category) to (broker, category, currency).

    Needed to support per-currency margin tracking (e.g. in_house USD/HKD/JPY/CNY).
    Idempotent — checks if migration is needed first.
    """
    # Check current UNIQUE constraint
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='margin_balances'"
    ).fetchone()
    if not row:
        return
    ddl = row[0] if isinstance(row, (tuple, list)) else row['sql']
    if 'UNIQUE(broker, category, currency)' in ddl:
        return  # already migrated

    conn.execute("""
        CREATE TABLE IF NOT EXISTS margin_balances_new (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            broker     TEXT NOT NULL,
            category   TEXT NOT NULL,
            currency   TEXT NOT NULL DEFAULT 'CNY',
            amount     REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            UNIQUE(broker, category, currency)
        )
    """)
    conn.execute("""
        INSERT OR IGNORE INTO margin_balances_new (broker, category, currency, amount, updated_at)
        SELECT broker, category, currency, amount, updated_at FROM margin_balances
    """)
    conn.execute("DROP TABLE margin_balances")
    conn.execute("ALTER TABLE margin_balances_new RENAME TO margin_balances")
    conn.commit()


def _migrate_snapshot_market_data(conn):
    """Add market_data JSON column to daily_snapshots (idempotent)."""
    try:
        conn.execute("ALTER TABLE daily_snapshots ADD COLUMN market_data TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists


def _migrate_positions_created_at(conn):
    """Add created_at column to positions (idempotent).

    created_at = immutable creation timestamp (never changes on edit).
    Existing rows get their current updated_at as created_at.
    """
    try:
        conn.execute("ALTER TABLE positions ADD COLUMN created_at TEXT")
    except sqlite3.OperationalError:
        return  # column already exists
    # Back-fill: existing positions use updated_at as created_at
    conn.execute("UPDATE positions SET created_at = updated_at WHERE created_at IS NULL")
    conn.commit()


def _migrate_snapshot_capital(conn):
    """Add capital column to daily_snapshots (idempotent)."""
    try:
        conn.execute("ALTER TABLE daily_snapshots ADD COLUMN capital REAL")
    except sqlite3.OperationalError:
        pass  # column already exists


def _migrate_normalize_broker_names(conn):
    """Normalize broker names in closed_trades to match positions table (idempotent)."""
    conn.execute("UPDATE closed_trades SET broker = '中信' WHERE broker = '中信证券'")
    conn.execute("UPDATE closed_trades SET broker = '招商' WHERE broker = '招商证券'")
    conn.commit()


def _migrate_backfill_realized_pnl_cny(conn):
    """Backfill realized_pnl_cny for existing closed trades (idempotent).

    Uses fx_rates from DB (or defaults) to convert original-currency P&L to CNY.
    Also reverts 粤高速B (id=349) if it was incorrectly changed to CNY.
    """
    # Skip if already backfilled (check if any non-NULL realized_pnl_cny)
    count = conn.execute(
        "SELECT COUNT(*) FROM closed_trades WHERE realized_pnl_cny IS NOT NULL"
    ).fetchone()[0]
    if count > 0:
        return

    # Read FX rates from DB, with defaults
    fx = {'CNY': 1.0, 'USD': 6.897, 'HKD': 0.882, 'JPY': 0.04378}
    for row in conn.execute("SELECT currency, rate_to_cny FROM fx_rates"):
        fx[row[0]] = row[1]

    # Revert 粤高速B if it was changed to CNY (original was HKD 522.24)
    r349 = conn.execute("SELECT currency, realized_pnl FROM closed_trades WHERE id=349").fetchone()
    if r349 and r349[0] == 'CNY' and abs(r349[1] - 460.62) < 1:
        conn.execute("UPDATE closed_trades SET currency='HKD', realized_pnl=522.24 WHERE id=349")

    # Backfill all rows
    for row in conn.execute("SELECT id, currency, realized_pnl FROM closed_trades"):
        rate = fx.get(row['currency'], 1.0)
        pnl_cny = row['realized_pnl'] * rate
        conn.execute("UPDATE closed_trades SET realized_pnl_cny=? WHERE id=?", (pnl_cny, row['id']))
    conn.commit()


def _migrate_backfill_market_detail(conn):
    """Backfill snapshot_market_detail from existing market_data JSON (idempotent).

    Handles both old format {"A股": 12345} and new format {"A股": {"CNY": 12345}}.
    Old-format values are stored as currency='CNY' (since they were already in CNY).
    """
    import json
    # Skip if already backfilled (check if any rows exist)
    count = conn.execute("SELECT COUNT(*) FROM snapshot_market_detail").fetchone()[0]
    if count > 0:
        return

    rows = conn.execute(
        "SELECT date, market_data FROM daily_snapshots WHERE market_data IS NOT NULL"
    ).fetchall()
    for date_val, md_json in rows:
        try:
            data = json.loads(md_json)
        except Exception:
            continue
        for market, val in data.items():
            if isinstance(val, dict):
                # New format: {currency: mv}
                for cur, mv in val.items():
                    conn.execute("""
                        INSERT OR IGNORE INTO snapshot_market_detail (date, market, currency, mv)
                        VALUES (?, ?, ?, ?)
                    """, (date_val, market, cur, mv))
            else:
                # Old format: value is already in CNY
                conn.execute("""
                    INSERT OR IGNORE INTO snapshot_market_detail (date, market, currency, mv)
                    VALUES (?, ?, 'CNY', ?)
                """, (date_val, market, val))
    conn.commit()


# ── Capital constants (from environment, see .env.example) ──
FUTU_CAPITAL    = float(os.environ.get('FUTU_CAPITAL', '0'))       # 富途历史人民币入金总额
FUTU_DEPOSIT_FX = float(os.environ.get('FUTU_DEPOSIT_FX', '1.0')) # 富途入金平均换汇汇率 USD/CNY
B_SHARE_CAPITAL = float(os.environ.get('B_SHARE_CAPITAL', '0'))   # B股历史入金总额

# Auto-detect capital mode:
#   deposit mode — when FUTU_CAPITAL or B_SHARE_CAPITAL is set (advanced, for specific brokers)
#   cost mode    — default, capital = sum of all position costs (simple, for new users)
DEPOSIT_MODE = FUTU_CAPITAL > 0 or B_SHARE_CAPITAL > 0


def compute_capital(conn, fx):
    """Compute total invested capital (CNY).

    Deposit mode (FUTU_CAPITAL > 0 or B_SHARE_CAPITAL > 0):
        Capital = 富途入金 + B股入金 + 其他持仓成本 + 现金 − 场外杠杆 − 已平仓盈亏(A股+基金+港股招商)

    Cost mode (default for new users):
        Capital = 全部持仓成本 + 现金 − 场外杠杆 − 全部已平仓盈亏
    """
    # Position cost
    position_cost = 0.0
    for row in conn.execute(
            "SELECT broker, market, currency, quantity, cost_price "
            "FROM positions WHERE status='open'"):
        if DEPOSIT_MODE and (row['broker'] == '富途' or row['market'] == 'B股'):
            continue  # covered by FUTU_CAPITAL / B_SHARE_CAPITAL
        rate = fx.get(row['currency'], 1.0)
        position_cost += row['quantity'] * row['cost_price'] * rate

    # Cash
    cash_cny = 0.0
    for row in conn.execute("SELECT currency, balance FROM cash_balances"):
        cash_cny += row['balance'] * fx.get(row['currency'], 1.0)

    # Off-exchange leverage
    margin_off = 0.0
    for row in conn.execute(
            "SELECT currency, amount FROM margin_balances WHERE category='off_exchange'"):
        margin_off += row['amount'] * fx.get(row['currency'], 1.0)

    # Realised P&L
    realised_pl = 0.0
    if DEPOSIT_MODE:
        # Deposit mode: only A股 + 基金 + 港股(招商) — 富途 P&L tracked via fixed deposit
        for row in conn.execute(
                "SELECT COALESCE(realized_pnl_cny, 0) AS rpl FROM closed_trades "
                "WHERE market IN ('A股', '基金') OR (market = '港股' AND broker = '招商')"):
            realised_pl += row['rpl']
    else:
        # Cost mode: ALL closed trades
        for row in conn.execute(
                "SELECT COALESCE(realized_pnl_cny, 0) AS rpl FROM closed_trades"):
            realised_pl += row['rpl']

    base = (FUTU_CAPITAL + B_SHARE_CAPITAL + position_cost) if DEPOSIT_MODE else position_cost
    return base + cash_cny - margin_off - realised_pl


def init_db(db_path=None):
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate_closed_trades(conn)
        _migrate_margin_multi_currency(conn)
        _migrate_snapshot_market_data(conn)
        _migrate_positions_created_at(conn)
        _migrate_snapshot_capital(conn)
        _migrate_normalize_broker_names(conn)
        _migrate_backfill_realized_pnl_cny(conn)
        _migrate_backfill_market_detail(conn)
        _migrate_ytd_add_qty_cost(conn)
        _migrate_seed_ytd_2026(conn)


def upsert_position(conn, ticker, name, market, broker, currency, quantity, cost_price):
    conn.execute("""
        INSERT INTO positions (ticker, name, market, broker, currency, quantity, cost_price,
                               status, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'open',
                datetime('now','localtime'), datetime('now','localtime'))
        ON CONFLICT(ticker, broker) DO UPDATE SET
            name       = excluded.name,
            market     = excluded.market,
            currency   = excluded.currency,
            quantity   = excluded.quantity,
            cost_price = excluded.cost_price,
            updated_at = CASE
                WHEN positions.quantity != excluded.quantity
                  OR abs(positions.cost_price - excluded.cost_price) > 0.0001
                THEN datetime('now','localtime')
                ELSE positions.updated_at
            END
            -- created_at is NEVER touched on update
    """, (ticker, name, market, broker, currency, quantity, cost_price))


def insert_closed_trade(conn, ticker, name, market, broker, currency, realized_pnl,
                        close_date=None, notes=None, quantity=None, cost_price=None,
                        close_price=None, cost_total=None, realized_pnl_cny=None):
    conn.execute("""
        INSERT INTO closed_trades (ticker, name, market, broker, currency, realized_pnl,
                                   close_date, notes, quantity, cost_price, close_price,
                                   cost_total, realized_pnl_cny)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ticker, name, market, broker, currency, realized_pnl,
          close_date, notes, quantity, cost_price, close_price, cost_total, realized_pnl_cny))


def upsert_cash(conn, account, currency, balance):
    conn.execute("""
        INSERT INTO cash_balances (account, currency, balance, updated_at)
        VALUES (?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(account, currency) DO UPDATE SET
            balance = excluded.balance,
            updated_at = excluded.updated_at
    """, (account, currency, balance))


def upsert_nav(conn, date, nav, capital, pnl, equity_nav=None, benchmark=None):
    conn.execute("""
        INSERT INTO nav_history (date, net_asset_value, capital_invested, pnl, equity_nav, benchmark_value)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            net_asset_value  = excluded.net_asset_value,
            capital_invested = excluded.capital_invested,
            pnl              = excluded.pnl,
            equity_nav       = excluded.equity_nav,
            benchmark_value  = excluded.benchmark_value
    """, (date, nav, capital, pnl, equity_nav, benchmark))


def upsert_fx(conn, currency, rate):
    conn.execute("""
        INSERT INTO fx_rates (currency, rate_to_cny, updated_at)
        VALUES (?, ?, datetime('now','localtime'))
        ON CONFLICT(currency) DO UPDATE SET
            rate_to_cny = excluded.rate_to_cny,
            updated_at  = excluded.updated_at
    """, (currency, rate))


def upsert_margin(conn, broker, category, currency, amount):
    conn.execute("""
        INSERT INTO margin_balances (broker, category, currency, amount, updated_at)
        VALUES (?, ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(broker, category, currency) DO UPDATE SET
            amount     = excluded.amount,
            updated_at = excluded.updated_at
    """, (broker, category, currency, amount))


def upsert_snapshot(conn, date, total_assets, net_assets, equity_mv, cash, leverage, total_pnl,
                    market_data=None, capital=None, market_detail=None):
    """Record daily snapshot — first write of the day wins (no overwrite).

    This ensures a stable baseline for "Last 1d" / weekly comparisons:
    the snapshot always reflects the portfolio state at the first load of the day.

    market_detail: dict {market: {currency: mv}} — structured per-market data.
                   Written to snapshot_market_detail table alongside the JSON blob.
    """
    conn.execute("""
        INSERT OR IGNORE INTO daily_snapshots
            (date, total_assets, net_assets, equity_mv_cny, cash_cny,
             leverage_cny, total_pnl_cny, market_data, capital, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
    """, (date, total_assets, net_assets, equity_mv, cash, leverage, total_pnl, market_data, capital))

    # Also write structured market detail (first-write-wins via INSERT OR IGNORE)
    if market_detail:
        for market, cur_dict in market_detail.items():
            if isinstance(cur_dict, dict):
                for currency, mv in cur_dict.items():
                    conn.execute("""
                        INSERT OR IGNORE INTO snapshot_market_detail (date, market, currency, mv)
                        VALUES (?, ?, ?, ?)
                    """, (date, market, currency, mv))


def get_ytd_baselines(conn, year):
    """Get YTD baseline data for a given year.

    Returns {ticker: {'price': float, 'quantity': float|None, 'cost_price': float|None}}.
    """
    rows = conn.execute(
        "SELECT ticker, price, quantity, cost_price FROM ytd_baseline_prices WHERE year = ?",
        (year,),
    ).fetchall()
    result = {}
    for r in rows:
        tk = r['ticker'] if isinstance(r, sqlite3.Row) else r[0]
        result[tk] = {
            'price': r['price'] if isinstance(r, sqlite3.Row) else r[1],
            'quantity': r['quantity'] if isinstance(r, sqlite3.Row) else r[2],
            'cost_price': r['cost_price'] if isinstance(r, sqlite3.Row) else r[3],
        }
    return result


def record_ytd_baselines(conn, year, ticker_data, date):
    """Bulk-insert YTD baseline prices (INSERT OR IGNORE — first write wins).

    ticker_data: dict {ticker: (price, currency)} or {ticker: (price, currency, qty, cost_price)}
    """
    for ticker, vals in ticker_data.items():
        price, currency = vals[0], vals[1]
        qty = vals[2] if len(vals) > 2 else None
        cost = vals[3] if len(vals) > 3 else None
        conn.execute("""
            INSERT OR IGNORE INTO ytd_baseline_prices
                (year, ticker, price, currency, date, quantity, cost_price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (year, ticker, price, currency, date, qty, cost))


# ── Migration: seed 2026 YTD baselines from hardcoded data ──
_YTD_2026_SEED = {
    # A股
    '601595.SS': (26.54, 'CNY'), '601816.SS': (5.05, 'CNY'),
    '600415.SS': (14.22, 'CNY'), '600809.SS': (160.71, 'CNY'),
    '605338.SS': (26.53, 'CNY'), '603605.SS': (68.17, 'CNY'),
    '002293.SZ': (10.41, 'CNY'),
    # B股
    '900928.SS': (0.697, 'USD'), '200596.SZ': (71.38, 'HKD'),
    '900903.SS': (0.203, 'USD'), '201872.SZ': (16.02, 'HKD'),
    '900905.SS': (3.478, 'USD'), '900936.SS': (1.229, 'USD'),
    # 基金
    '001071': (3.929, 'CNY'),
    # 日股
    '8031.T': (5931.0, 'JPY'), '8058.T': (5073.0, 'JPY'),
    '8002.T': (5383.0, 'JPY'), '8001.T': (2089.5, 'JPY'),
    '8053.T': (5871.0, 'JPY'),
    # 港股
    '3968.HK': (48.8, 'HKD'), '0005.HK': (135.2, 'HKD'),
    '0300.HK': (86.05, 'HKD'), '0700.HK': (519.0, 'HKD'),
    '1060.HK': (0.77, 'HKD'), '2020.HK': (80.4, 'HKD'),
    '1810.HK': (33.42, 'HKD'), '2367.HK': (30.5, 'HKD'),
    '0772.HK': (30.44, 'HKD'), '6936.HK': (35.08, 'HKD'),
    '1913.HK': (40.88, 'HKD'),
    # 美股
    'CRCL': (101.91, 'USD'), 'FXI': (35.82, 'USD'), 'TSM': (338.89, 'USD'),
    'FUTU': (143.46, 'USD'), 'RL': (338.36, 'USD'), 'STZ': (146.47, 'USD'),
    'VOO': (618.43, 'USD'), 'DIDIY': (4.18, 'USD'), 'TSLA': (396.73, 'USD'),
    'QQQ': (599.75, 'USD'), 'TME': (13.62, 'USD'), 'NVDA': (177.82, 'USD'),
    'INTC': (43.42, 'USD'), 'AAPL': (257.46, 'USD'), 'GOOGL': (298.52, 'USD'),
}


def _migrate_ytd_add_qty_cost(conn):
    """Add quantity and cost_price columns to ytd_baseline_prices (idempotent).
    Backfill from current positions table (approximate for 2026 seed)."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(ytd_baseline_prices)").fetchall()]
    if 'quantity' in cols:
        return
    conn.execute("ALTER TABLE ytd_baseline_prices ADD COLUMN quantity REAL")
    conn.execute("ALTER TABLE ytd_baseline_prices ADD COLUMN cost_price REAL")
    # Backfill from current positions — best approximation for historical seed
    conn.execute("""
        UPDATE ytd_baseline_prices
        SET quantity = (
                SELECT SUM(p.quantity) FROM positions p
                WHERE p.ticker = ytd_baseline_prices.ticker AND p.status = 'open'
            ),
            cost_price = (
                SELECT p.cost_price FROM positions p
                WHERE p.ticker = ytd_baseline_prices.ticker AND p.status = 'open'
                LIMIT 1
            )
    """)
    conn.commit()


def _migrate_seed_ytd_2026(conn):
    """Seed 2026 YTD baseline prices from hardcoded data (idempotent)."""
    count = conn.execute(
        "SELECT COUNT(*) FROM ytd_baseline_prices WHERE year = 2026"
    ).fetchone()[0]
    if count > 0:
        return  # already seeded
    record_ytd_baselines(conn, 2026, _YTD_2026_SEED, '2026-03-06')
    conn.commit()


if __name__ == '__main__':
    init_db()
    print(f"Database initialized at {DB_PATH}")
