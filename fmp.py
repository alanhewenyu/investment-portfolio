"""Industry classification with multi-source fallback and SQLite caching.

Priority chain:
  1. ETF mapping (static, for known index ETFs)
  2. SQLite cache (30-day TTL)
  3. FMP API (if FMP_API_KEY is set)
  4. akshare (Chinese A/B-shares and mutual funds — free, via 东方财富)
  5. yfinance (US, HK, JP stocks — free)

API key: set FMP_API_KEY environment variable (optional).
Cache: 30-day TTL in industry_cache table (industry rarely changes).
"""

from __future__ import annotations

import logging
import os
import time
import sqlite3
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

from db import DB_PATH

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/api/v3"
_CACHE_TTL = 86400 * 30  # 30 days

# ETF description mapping — describes what the ETF tracks (for display)
_ETF_INDUSTRY = {
    'SPY':  ('ETF', 'S&P 500 Index'),
    'QQQ':  ('ETF', 'Nasdaq 100 Index'),
    'FXI':  ('ETF', 'China Large-Cap'),
}


# ── Cache helpers ─────────────────────────────────────────

def _ensure_table(db_path=None):
    with sqlite3.connect(db_path or DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS industry_cache (
                ticker     TEXT PRIMARY KEY,
                sector     TEXT,
                industry   TEXT,
                fetched_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
        """)


def _get_cached(ticker, db_path=None):
    """Return (sector, industry) from cache if fresh, else None."""
    try:
        with sqlite3.connect(db_path or DB_PATH) as conn:
            row = conn.execute(
                "SELECT sector, industry, fetched_at FROM industry_cache WHERE ticker=?",
                (ticker,),
            ).fetchone()
        if row:
            fetched = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            if (datetime.now() - fetched).total_seconds() < _CACHE_TTL:
                return row[0], row[1]
    except Exception:
        pass
    return None


def _cache_result(ticker, sector, industry, db_path=None):
    try:
        with sqlite3.connect(db_path or DB_PATH) as conn:
            conn.execute("""
                INSERT INTO industry_cache (ticker, sector, industry, fetched_at)
                VALUES (?, ?, ?, datetime('now','localtime'))
                ON CONFLICT(ticker) DO UPDATE SET
                    sector     = excluded.sector,
                    industry   = excluded.industry,
                    fetched_at = excluded.fetched_at
            """, (ticker, sector, industry))
    except Exception:
        pass


# ── API fetch ─────────────────────────────────────────────

def _is_chinese_ticker(ticker: str) -> bool:
    """Check if ticker is a Chinese stock (.SS/.SZ) or mutual fund (6-digit code)."""
    if ticker.endswith('.SS') or ticker.endswith('.SZ'):
        return True
    # Pure 6-digit numeric code → likely a Chinese mutual fund
    if len(ticker) == 6 and ticker.isdigit():
        return True
    return False


def _akshare_fallback(ticker: str) -> tuple[str, str]:
    """Fetch (sector, industry) for Chinese stocks/funds via akshare (free).

    Covers A-shares, B-shares (.SS/.SZ) and Chinese mutual funds (6-digit codes).
    Data source: 东方财富 via akshare library.
    """
    try:
        import akshare as ak
    except ImportError:
        return '', ''

    # ── Chinese stock (A-share or B-share) ──
    if ticker.endswith('.SS') or ticker.endswith('.SZ'):
        code = ticker.split('.')[0]  # Strip .SS/.SZ suffix for akshare
        try:
            df = ak.stock_individual_info_em(symbol=code)
            row = df[df['item'] == '行业']
            if not row.empty:
                industry = str(row.iloc[0]['value']).strip()
                if industry and industry != 'nan':
                    return industry, industry
        except Exception:
            pass
        return '', ''

    # ── Chinese mutual fund (6-digit code, no suffix) ──
    if len(ticker) == 6 and ticker.isdigit():
        try:
            df = ak.fund_individual_basic_info_xq(symbol=ticker)
            row = df[df['item'] == '基金类型']
            if not row.empty:
                fund_type = str(row.iloc[0]['value']).strip()
                if fund_type and fund_type != 'nan':
                    return '基金', fund_type
        except Exception:
            pass
        return '', ''

    return '', ''


def _yfinance_fallback(ticker: str) -> tuple[str, str]:
    """Free fallback: fetch (sector, industry) via yfinance.

    Works for US stocks, HK (.HK), Japan (.T).
    Chinese A-shares, B-shares, and funds are handled by _akshare_fallback().
    """
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        sector = info.get('sector', '') or ''
        industry = info.get('industry', '') or ''
        if sector in ('', 'N/A'):
            sector = ''
        if industry in ('', 'N/A'):
            industry = ''
        return sector, industry
    except Exception:
        return '', ''


def _to_fmp_ticker(ticker):
    """Convert Yahoo Finance ticker format to FMP format if needed."""
    if not ticker:
        return ticker
    # FMP uses .SS for Shanghai (same as Yahoo), .SZ for Shenzhen, .HK, .T — all same
    return ticker


def fetch_profile(ticker: str, db_path: str | None = None) -> tuple[str, str]:
    """Fetch (sector, industry) for a single ticker. Uses cache first."""
    if not ticker:
        return '', ''

    # Check explicit ETF mapping first (overrides cache)
    if ticker in _ETF_INDUSTRY:
        sector, industry = _ETF_INDUSTRY[ticker]
        cached = _get_cached(ticker, db_path)
        if cached != (sector, industry):
            _cache_result(ticker, sector, industry, db_path)
        return sector, industry

    cached = _get_cached(ticker, db_path)
    if cached and (cached[0] or cached[1]):
        return cached

    # Try FMP API if key is available
    if FMP_API_KEY:
        fmp_ticker = _to_fmp_ticker(ticker)
        try:
            resp = requests.get(
                f"{FMP_BASE}/profile/{fmp_ticker}",
                params={"apikey": FMP_API_KEY},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                profile = data[0]
                is_etf = profile.get('isEtf', False)
                sector = profile.get('sector', '') or ''
                industry = profile.get('industry', '') or ''
                # Override ETFs: FMP returns "Asset Management" for the fund manager,
                # but we want to classify the ETF by what it tracks
                if is_etf or industry == 'Asset Management':
                    company_name = profile.get('companyName', '') or ''
                    sector = 'ETF'
                    industry = company_name if company_name else 'ETF'
                if sector or industry:
                    _cache_result(ticker, sector, industry, db_path)
                    return sector, industry
        except Exception as e:
            logger.warning("FMP profile fetch failed for %s: %s", fmp_ticker, e)

    # Free fallback: akshare for Chinese stocks/funds
    if _is_chinese_ticker(ticker):
        sector, industry = _akshare_fallback(ticker)
        if sector or industry:
            _cache_result(ticker, sector, industry, db_path)
            return sector, industry

    # Free fallback: yfinance (works for US, HK, JP stocks — no API key needed)
    sector, industry = _yfinance_fallback(ticker)
    if sector or industry:
        _cache_result(ticker, sector, industry, db_path)
        return sector, industry

    # Cache empty result to avoid repeated failures
    _cache_result(ticker, '', '', db_path)
    return '', ''


def _get_all_cached(tickers, db_path=None):
    """Batch fetch all cached entries in a single DB query (vs N individual opens)."""
    if not tickers:
        return {}
    try:
        with sqlite3.connect(db_path or DB_PATH) as conn:
            placeholders = ','.join('?' for _ in tickers)
            rows = conn.execute(
                f"SELECT ticker, sector, industry, fetched_at FROM industry_cache "
                f"WHERE ticker IN ({placeholders})",
                tickers,
            ).fetchall()
        results = {}
        now = datetime.now()
        for row in rows:
            fetched = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')  # fetched_at
            if (now - fetched).total_seconds() < _CACHE_TTL:
                results[row[0]] = (row[1], row[2])
        return results
    except Exception:
        return {}


def fetch_all_industries(tickers: list[str], db_path: str | None = None) -> dict[str, tuple[str, str]]:
    """Batch fetch sector/industry for a list of tickers.

    Returns {ticker: (sector, industry)}.
    Uses DB cache aggressively; only hits API for uncached tickers.
    Rate-limited at ~5 req/sec.
    """
    _ensure_table(db_path)

    # Single batch query instead of N individual connections
    clean_tickers = [t for t in tickers if t]
    cached_batch = _get_all_cached(clean_tickers, db_path)

    results = {}
    uncached = []

    for t in clean_tickers:
        # Check ETF mapping first (overrides cache)
        if t in _ETF_INDUSTRY:
            results[t] = _ETF_INDUSTRY[t]
        elif t in cached_batch and (cached_batch[t][0] or cached_batch[t][1]):
            results[t] = cached_batch[t]
        else:
            uncached.append(t)

    # Fetch uncached tickers with rate limiting
    for i, t in enumerate(uncached):
        sector, industry = fetch_profile(t, db_path)
        results[t] = (sector, industry)
        if i < len(uncached) - 1:
            time.sleep(0.22)  # ~4.5 req/sec to stay within limits

    return results
