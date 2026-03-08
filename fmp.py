"""FMP API client for sector/industry classification with SQLite caching.

Uses Financial Modeling Prep v3 API to fetch company profiles.
API key: set FMP_API_KEY environment variable.
Cache: 30-day TTL in industry_cache table (industry rarely changes).
"""

import os
import sys
import time
import sqlite3
from datetime import datetime

import requests

from db import DB_PATH

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/api/v3"
_CACHE_TTL = 86400 * 30  # 30 days

# Manual industry mapping for tickers FMP can't resolve (B-shares, funds, etc.)
# B-share industry is same as corresponding A-share
_MANUAL_INDUSTRY = {
    '900928.SS': ('Real Estate', 'Real Estate - Services'),           # 临港B
    '900905.SS': ('Consumer Cyclical', 'Luxury Goods'),               # 老凤祥B → 600612.SS
    '900936.SS': ('Consumer Cyclical', 'Textile Manufacturing'),      # 鄂资B (鄂绒)
    '900903.SS': ('Utilities', 'Regulated Gas'),                      # 大众B → 600635.SS
    '201872.SZ': ('Industrials', 'Marine Shipping'),                  # 招港B → 001872.SZ
    # Chinese funds / OTC funds — FMP can't resolve these
    '001071':    ('ETF', 'Mixed - TMT'),                              # 华安媒体互联网混合
}

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

def _yfinance_fallback(ticker):
    """Free fallback: fetch (sector, industry) via yfinance.

    Works for US stocks, HK (.HK), Japan (.T).
    Chinese A-shares (.SS/.SZ), funds, B-shares are covered by
    _MANUAL_INDUSTRY and should never reach this function.
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


def fetch_profile(ticker, db_path=None):
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

    # Check manual mapping (B-shares, funds, etc.)
    if ticker in _MANUAL_INDUSTRY:
        sector, industry = _MANUAL_INDUSTRY[ticker]
        _cache_result(ticker, sector, industry, db_path)
        return sector, industry

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
            print(f"  FMP profile fetch failed for {fmp_ticker}: {e}", file=sys.stderr)

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


def fetch_all_industries(tickers, db_path=None):
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
        # Check ETF / manual mappings first (these override cache)
        if t in _ETF_INDUSTRY:
            results[t] = _ETF_INDUSTRY[t]
        elif t in _MANUAL_INDUSTRY:
            results[t] = _MANUAL_INDUSTRY[t]
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
