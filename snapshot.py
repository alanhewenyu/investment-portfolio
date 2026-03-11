#!/usr/bin/env python3
"""Daily portfolio snapshot — designed to run via cron at 06:00 CST.

Captures end-of-day NAV, equity, cash, leverage, and P&L for all markets.
All prices are fetched fresh (no Streamlit cache). Results written to daily_snapshots.

Usage:
    python3 snapshot.py              # snapshot for today (default)
    python3 snapshot.py --dry-run    # print without writing to DB

Cron (run at 06:00 Beijing time every day):
    0 6 * * * cd /Users/Alan/portfolio-tracker && /usr/bin/env python3 snapshot.py >> snapshot.log 2>&1
"""

import json
import shutil
import sys
import os
import sqlite3
from datetime import datetime
from pathlib import Path

# Ensure project dir is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import DB_PATH, get_conn, upsert_snapshot, upsert_fx, init_db, compute_capital
from prices import fetch_price, fetch_fx_rate


def _fmt(val):
    return f"{val:,.0f}" if val is not None else "—"


def take_snapshot(dry_run=False):
    """Fetch all prices, compute NAV, write snapshot."""
    # Skip Sun/Mon — US market closed Sat/Sun, Beijing time is +1 day
    # Tue–Sat snapshots at 6am capture Mon–Fri US close (trading_date = snapshot − 1)
    if datetime.now().weekday() in (6, 0):  # 6=Sun, 0=Mon
        print(f"[{datetime.now():%Y-%m-%d %H:%M}] No trading day (Sun/Mon), skipping.")
        return None
    init_db()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*55}")
    print(f"Portfolio Snapshot  {ts}")
    print(f"{'='*55}")

    conn = get_conn()

    # ── FX rates ──
    fx = {"CNY": 1.0}
    for cur in ("USD", "HKD", "JPY"):
        rate = fetch_fx_rate(cur)
        if rate and rate > 0:
            fx[cur] = rate
            if not dry_run:
                upsert_fx(conn, cur, rate)
        else:
            # Fallback: read last saved rate
            row = conn.execute(
                "SELECT rate_to_cny FROM fx_rates WHERE currency=?", (cur,)
            ).fetchone()
            if row:
                fx[cur] = row[0]
                print(f"  WARN: live FX failed for {cur}, using DB fallback {row[0]}")
            else:
                print(f"  WARN: no FX rate for {cur}, defaulting to 1.0")
                fx[cur] = 1.0

    print(f"FX: USD={fx.get('USD',0):.4f}  HKD={fx.get('HKD',0):.5f}  JPY={fx.get('JPY',0):.5f}")

    # ── Positions ──
    positions = conn.execute("""
        SELECT ticker, name, market, broker, currency, quantity, cost_price
        FROM positions WHERE status='open'
    """).fetchall()

    equity_mv = 0.0
    total_cost_cny = 0.0
    total_pnl_cny = 0.0
    stale_count = 0
    market_mv = {}  # per-market equity MV in CNY

    # Parallel price fetch — ~10x faster with 45+ positions
    from concurrent.futures import ThreadPoolExecutor
    _tickers = [pos["ticker"] for pos in positions]
    def _fetch_one(t):
        p, _ = fetch_price(t, regular_only=True)
        return p
    try:
        with ThreadPoolExecutor(max_workers=min(len(_tickers) or 1, 8)) as pool:
            _fetched = list(pool.map(_fetch_one, _tickers, timeout=60))
    except Exception:
        _fetched = [None] * len(_tickers)

    for i, pos in enumerate(positions):
        ticker, name, market, broker, currency, qty, cost_price = (
            pos["ticker"], pos["name"], pos["market"], pos["broker"],
            pos["currency"], pos["quantity"], pos["cost_price"],
        )
        rate = fx.get(currency, 1.0)

        price = _fetched[i]
        if price is None:
            price = cost_price
            stale_count += 1

        mv_cny = qty * price * rate
        cost_cny = qty * cost_price * rate
        pnl_cny = mv_cny - cost_cny

        equity_mv += mv_cny
        total_cost_cny += cost_cny
        total_pnl_cny += pnl_cny
        market_mv[market] = market_mv.get(market, 0) + mv_cny

    print(f"\nPositions: {len(positions)} ({stale_count} stale prices)")
    print(f"Equity MV:  ¥{_fmt(equity_mv)}")

    # ── Guard: abort if too many prices are stale (broken environment) ──
    if positions and stale_count / len(positions) > 0.5:
        print(f"\n✗ ABORT: {stale_count}/{len(positions)} prices stale — "
              f"environment likely broken (yfinance missing?). Snapshot NOT saved.")
        conn.close()
        return None

    # ── Cash ──
    cash_cny = 0.0
    for row in conn.execute("SELECT currency, balance FROM cash_balances"):
        cash_cny += row["balance"] * fx.get(row["currency"], 1.0)
    print(f"Cash:       ¥{_fmt(cash_cny)}")

    # ── Leverage ──
    in_house = 0.0
    off_exchange = 0.0
    for row in conn.execute("SELECT category, currency, amount FROM margin_balances"):
        rate = fx.get(row["currency"], 1.0)
        amt_cny = row["amount"] * rate
        if row["category"] == "in_house":
            in_house += amt_cny
        elif row["category"] == "off_exchange":
            off_exchange += amt_cny
    total_leverage = in_house + off_exchange
    print(f"Leverage:   ¥{_fmt(total_leverage)} (in={_fmt(in_house)}, off={_fmt(off_exchange)})")

    # ── Metrics ──
    total_assets = equity_mv + cash_cny
    net_assets = total_assets - total_leverage
    pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny > 0 else 0

    print(f"\nTotal Assets: ¥{_fmt(total_assets)}")
    print(f"Net Assets:   ¥{_fmt(net_assets)}")
    print(f"Unrealized:   ¥{_fmt(total_pnl_cny)} ({pnl_pct:+.1f}%)")

    # ── Capital ──
    capital = compute_capital(conn, fx)
    print(f"Capital:      ¥{_fmt(capital)}")

    # ── Write snapshot ──
    today = datetime.now().strftime("%Y-%m-%d")
    market_json = json.dumps(market_mv, ensure_ascii=False)
    print(f"Market MV:    {market_json}")

    if dry_run:
        print(f"\n[DRY RUN] Would write snapshot for {today}")
    else:
        upsert_snapshot(conn, today, total_assets, net_assets,
                        equity_mv, cash_cny, total_leverage, total_pnl_cny,
                        market_data=market_json, capital=capital)
        conn.commit()
        print(f"\n✓ Snapshot saved for {today}")

    conn.close()

    return {
        "date": today,
        "total_assets": total_assets,
        "net_assets": net_assets,
        "equity_mv": equity_mv,
        "cash_cny": cash_cny,
        "leverage": total_leverage,
        "pnl": total_pnl_cny,
    }


def backup_db(keep_daily=7):
    """Backup portfolio.db using SQLite backup API (safe, no corruption risk).

    Backup directory priority:
      1. BACKUP_DIR env var (user-configured)
      2. Default: ./backups/ (inside project directory)

    Tip: set BACKUP_DIR to a cloud-synced folder for automatic off-site backup,
    e.g. ~/Documents/backup/portfolio-tracker (iCloud on macOS).

    Retention policy:
      - Keep the last `keep_daily` daily backups (default: 7)
      - Keep the 1st-of-month backup indefinitely (monthly archive)
    """
    _env_dir = os.environ.get("BACKUP_DIR", "").strip()
    backup_dir = Path(_env_dir).expanduser() if _env_dir else Path(__file__).parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    src = Path(DB_PATH)
    if not src.exists():
        print(f"  WARN: DB not found at {src}, skipping backup")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    dst = backup_dir / f"portfolio_{today}.db"

    # Use sqlite3 backup API for a safe, consistent copy (no corruption risk)
    src_conn = sqlite3.connect(str(src))
    dst_conn = sqlite3.connect(str(dst))
    try:
        src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()
        size_mb = dst.stat().st_size / (1024 * 1024)
        print(f"✓ Backup saved: {dst}  ({size_mb:.1f} MB)")
    except Exception as e:
        dst_conn.close()
        src_conn.close()
        print(f"  WARN: backup failed: {e}")
        return

    # Also maintain a "latest" symlink / copy for easy restore
    latest = backup_dir / "portfolio_latest.db"
    shutil.copy2(str(dst), str(latest))

    # ── Retention: prune old daily backups, keep monthly (1st of month) ──
    backups = sorted(backup_dir.glob("portfolio_????-??-??.db"))
    for f in backups:
        name = f.stem  # portfolio_2026-03-08
        date_str = name.replace("portfolio_", "")
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        # Keep monthly archives (1st of month) indefinitely
        if dt.day == 1:
            continue
        # Keep recent daily backups
        age = (datetime.now() - dt).days
        if age > keep_daily:
            f.unlink()
            print(f"  Pruned old backup: {f.name}")


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    take_snapshot(dry_run=dry)
    if not dry:
        backup_db()
