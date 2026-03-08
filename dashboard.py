#!/usr/bin/env python3
"""Portfolio Dashboard — professional single-page portfolio view."""

import calendar
import sqlite3
import threading

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db import (DB_PATH, get_conn, init_db, upsert_position, upsert_snapshot,
                upsert_cash, upsert_margin, insert_closed_trade,
                compute_capital, FUTU_CAPITAL, FUTU_DEPOSIT_FX, B_SHARE_CAPITAL,
                DEPOSIT_MODE, get_ytd_baselines, record_ytd_baselines)
from prices import fetch_price, get_fx_rates, prefetch_all, get_previous_close
from fmp import fetch_all_industries

init_db()  # ensure migrations (e.g. created_at) are applied

# ── YTD baseline prices are stored in DB table `ytd_baseline_prices` ──
# 2026 seed: 3/6 (Fri) closing prices (migrated from hardcoded dict).
# Future years: auto-recorded on first snapshot of new year via prev_close.

st.set_page_config(page_title="Portfolio Tracker", page_icon="◼", layout="wide")

# ────────────────────────────────────────
# CSS
# ────────────────────────────────────────

st.markdown("""<style>
:root {
    --pf-bg:         #ffffff;
    --pf-bg2:        #f6f8fa;
    --pf-text:       #1f2328;
    --pf-text2:      #656d76;
    --pf-border:     #d0d7de;
    --pf-accent:     #0969da;
    --pf-green:      #cf222e;
    --pf-red:        #1a7f37;
    --pf-mono:       'SF Mono', 'Cascadia Code', 'Consolas', monospace;
}
@media (prefers-color-scheme: dark) {
    :root {
        --pf-bg:     #0d1117;
        --pf-bg2:    #161b22;
        --pf-text:   #e6edf3;
        --pf-text2:  #8b949e;
        --pf-border: #30363d;
        --pf-accent: #58a6ff;
        --pf-green:  #f85149;
        --pf-red:    #3fb950;
    }
}

/* KPI cards */
.kpi-row { display: flex; gap: 12px; margin-bottom: 12px; }
.kpi-card {
    flex: 1; padding: 14px 18px; border-radius: 8px;
    background: var(--pf-bg2); border: 1px solid var(--pf-border);
}
.kpi-label { font-size: 11px; color: var(--pf-text2); text-transform: uppercase;
             letter-spacing: 0.5px; margin-bottom: 3px; }
.kpi-value { font-size: 20px; font-weight: 600; font-family: var(--pf-mono);
             color: var(--pf-text); }
.kpi-sub   { font-size: 12px; font-family: var(--pf-mono); margin-top: 2px; }
.kpi-green { color: var(--pf-green); }
.kpi-red   { color: var(--pf-red); }

/* FX banner */
.fx-banner {
    display: flex; gap: 24px; margin-bottom: 14px;
    font-family: var(--pf-mono); font-size: 12px; color: var(--pf-text2);
}
.fx-banner b { color: var(--pf-text); }

/* Holdings table */
.holdings-wrap {
    overflow-x: auto; max-width: 100%;
    border: 1px solid var(--pf-border); border-radius: 8px;
    margin-bottom: 12px;
}
.holdings-table {
    width: max-content; min-width: 100%;
    border-collapse: separate; border-spacing: 0;
    font-family: var(--pf-mono); font-size: 13px;
}
.holdings-table th {
    text-align: left; padding: 6px 10px; font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.5px; color: var(--pf-text2); border-bottom: 1px solid var(--pf-border);
    background: var(--pf-bg); white-space: nowrap;
}
.holdings-table th.num { text-align: right; }
.holdings-table th.sub {
    font-size: 10px; font-weight: 400; text-transform: none; letter-spacing: 0;
    padding-top: 0; border-bottom: 2px solid var(--pf-border);
}
.holdings-table td {
    padding: 5px 10px; border-bottom: 1px solid var(--pf-border);
    color: var(--pf-text); white-space: nowrap; background: var(--pf-bg);
}
.holdings-table td.num { text-align: right; font-variant-numeric: tabular-nums; }
.holdings-table .ind-text {
    display: inline-block; max-width: 80px; overflow: hidden;
    text-overflow: ellipsis; vertical-align: bottom;
}
.holdings-table .after-freeze { padding-left: 16px; }
.holdings-table tbody tr:hover td {
    background: color-mix(in srgb, var(--pf-accent) 5%, var(--pf-bg));
}
.holdings-table .frozen { position: sticky; z-index: 2; }
.holdings-table thead .frozen { z-index: 3; }
.holdings-table .freeze-end { box-shadow: 2px 0 4px rgba(0,0,0,0.05); }
.holdings-table tbody tr:last-child td { border-bottom: none; }
.holdings-table tfoot td {
    border-top: 2px solid var(--pf-border); border-bottom: none;
    font-weight: 600; padding: 8px 10px;
}
.holdings-table .row-link {
    color: var(--pf-accent); cursor: pointer; font: inherit;
    font-family: var(--pf-mono); font-size: 13px;
}
.holdings-table .row-link:hover { text-decoration: underline; }
.pnl-pos, .holdings-table td.pnl-pos,
.holdings-table tfoot td.pnl-pos { color: var(--pf-green) !important; }
.pnl-neg, .holdings-table td.pnl-neg,
.holdings-table tfoot td.pnl-neg { color: var(--pf-red) !important; }
.price-stale { color: var(--pf-text2); font-style: italic; }
.red-dot {
    color: #cf222e; font-size: 8px; vertical-align: super;
    margin-left: 3px; cursor: help; position: relative;
}
.red-dot:hover::after {
    content: attr(data-tip); position: absolute; left: 50%; bottom: 120%;
    transform: translateX(-50%); white-space: nowrap;
    background: #333; color: #fff; font-size: 11px; padding: 3px 8px;
    border-radius: 4px; z-index: 99; pointer-events: none;
    font-style: normal; font-weight: 400; letter-spacing: 0;
}

/* Balanced padding on main area */
section.main > div.block-container {
    padding-left: 2.5rem !important; padding-right: 2.5rem !important;
    max-width: 100% !important;
}
.stMainBlockContainer {
    padding-left: 2.5rem !important; padding-right: 2.5rem !important;
    max-width: 100% !important;
}
/* Sidebar */
[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
    width: 100%; font-family: var(--pf-mono);
}


/* Hide +/- step buttons on sidebar number inputs (direct typing is faster for amounts) */
[data-testid="stSidebar"] [data-testid="stNumberInput"] button { display: none !important; }
[data-testid="stSidebar"] [data-testid="stNumberInput"] input { border-radius: 8px !important; }
/* Hide "Press Enter to submit form" instruction */
[data-testid="stSidebar"] [data-testid="InputInstructions"] { display: none !important; }
[data-testid="stSidebar"] .stForm [data-testid="InputInstructions"] { display: none !important; }

/* Section headers */
.section-title {
    font-size: 14px; font-weight: 600; color: var(--pf-text);
    text-transform: uppercase; letter-spacing: 0.5px;
    padding-bottom: 8px; border-bottom: 2px solid var(--pf-border);
    margin: 32px 0 16px 0;
}

/* Cash table */
.cash-table { width: 100%; border-collapse: collapse; font-family: var(--pf-mono); font-size: 13px; }
.cash-table th {
    text-align: right; padding: 6px 12px; font-size: 11px; text-transform: uppercase;
    color: var(--pf-text2); border-bottom: 2px solid var(--pf-border);
}
.cash-table th:first-child { text-align: left; }
.cash-table td { padding: 6px 12px; text-align: right; border-bottom: 1px solid var(--pf-border); color: var(--pf-text); }
.cash-table td:first-child { text-align: left; }

/* Hide Streamlit chrome but keep sidebar expand button visible */
header[data-testid="stHeader"] {
    background: transparent !important;
    height: auto !important;
}
/* Hide deploy button, status widget & main menu — keep toolbar for sidebar expand btn */
[data-testid="stStatusWidget"],
[data-testid="stHeader"] [data-testid="stDecoration"],
[data-testid="stToolbarActions"],
[data-testid="stAppDeployButton"],
#MainMenu { display: none !important; }
.block-container { padding-top: 24px; }

/* P&L calendar */
.pnl-cal { width: 100%; border-collapse: separate; border-spacing: 3px;
           font-family: var(--pf-mono); font-size: 12px; table-layout: fixed; }
.pnl-cal th { text-align: center; padding: 4px; font-size: 11px; color: var(--pf-text2);
              font-weight: 400; width: 14.285%; }
.pnl-cal td { text-align: center; padding: 6px 2px; border-radius: 6px; }
.pnl-cal td.has-data { cursor: default; }
.pnl-cal td.empty { color: var(--pf-text2); opacity: 0.3; }
.pnl-cal .day-num { font-weight: 600; line-height: 1.4; }
.pnl-cal .day-val { font-size: 10px; line-height: 1.2; }

[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] .stMarkdown h3 {
    font-size: 15px !important; font-family: var(--pf-mono) !important;
}
</style>""", unsafe_allow_html=True)

# ────────────────────────────────────────
# Data
# ────────────────────────────────────────

if 'prefetched' not in st.session_state:
    st.session_state.prefetched = True
    threading.Thread(target=prefetch_all, daemon=True).start()


def _query(sql, params=()):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn, params=params)


@st.cache_data(ttl=10, show_spinner=False)
def load_positions():
    return _query("SELECT * FROM positions WHERE status='open' ORDER BY market, broker, name")


@st.cache_data(ttl=10, show_spinner=False)
def load_closed():
    return _query("SELECT * FROM closed_trades ORDER BY market, abs(realized_pnl) DESC")


@st.cache_data(ttl=10, show_spinner=False)
def load_cash():
    return _query("SELECT * FROM cash_balances ORDER BY account")


@st.cache_data(ttl=60, show_spinner=False)
def load_nav():
    return _query("SELECT * FROM nav_history ORDER BY date")


@st.cache_data(ttl=10, show_spinner=False)
def load_margin():
    return _query("SELECT * FROM margin_balances")


@st.cache_data(ttl=30, show_spinner=False)
def load_snapshots():
    return _query("SELECT * FROM daily_snapshots ORDER BY date DESC")


def _fmt(val, decimals=0):
    if val is None or pd.isna(val):
        return '—'
    if decimals == 0:
        return f"{val:,.0f}"
    return f"{val:,.{decimals}f}"


def _pnl_class(val):
    if val is None or pd.isna(val):
        return ''
    return 'pnl-pos' if val >= 0 else 'pnl-neg'


def _pnl_sign(val, decimals=0):
    if val is None or pd.isna(val):
        return '—'
    prefix = '+' if val > 0 else ''
    return f"{prefix}{_fmt(val, decimals)}"


def _apply_fx_to_closed(closed_df, fx):
    """Recalculate realized_pnl_cny for 富途 accounts using live FX rates.

    富途 accounts hold foreign-currency assets (USD/HKD/JPY), so their realised P&L
    in CNY should reflect current exchange rates. Other accounts (中信, 招商, 支付宝,
    B股) had their P&L settled/recorded in CNY and should not change.
    """
    if closed_df.empty:
        return closed_df
    df = closed_df.copy()
    _fx_mask = df['broker'] == '富途'
    if _fx_mask.any():
        df.loc[_fx_mask, 'realized_pnl_cny'] = df.loc[_fx_mask].apply(
            lambda r: r['realized_pnl'] * fx.get(r['currency'], 1.0), axis=1
        )
    return df


# ────────────────────────────────────────
# Build enriched positions
# ────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def build_portfolio(fx_tuple=None):
    df = load_positions()
    if df.empty:
        return df
    fx = dict(fx_tuple) if fx_tuple else get_fx_rates()

    # Fetch industry data (cached in DB, 30-day TTL)
    tickers = df['ticker'].unique().tolist()
    industry_map = fetch_all_industries(tickers)

    # Parallel price prefetch — warm _price_cache for all tickers at once
    # (vs sequential fetch_price per ticker — ~10x faster with 45+ positions)
    from concurrent.futures import ThreadPoolExecutor as _TPE
    try:
        with _TPE(max_workers=min(len(tickers), 8)) as _pool:
            list(_pool.map(fetch_price, tickers, timeout=30))
    except Exception:
        pass  # partial cache is fine; remaining prices fetched on-demand below

    prices = []
    market_values = []
    market_values_cny = []
    regular_mvs = []         # prev_close-based MV (for snapshot baseline)
    costs_total = []
    pnls = []
    pnl_pcts = []
    pnl_cnys = []
    stale_flags = []
    daily_pnls = []
    daily_pnl_pcts = []
    daily_pnl_cnys = []
    ytd_pnls = []
    ytd_pnl_pcts = []
    ytd_pnl_cnys = []
    ytd_base_costs = []      # baseline cost (CNY) for total-row YTD P&L% denominator

    # Load YTD baselines from DB (auto-recorded at year start)
    _current_year = pd.Timestamp.now().year
    with get_conn() as _ytd_conn:
        _ytd_baselines = get_ytd_baselines(_ytd_conn, _current_year)

    for _, row in df.iterrows():
        ticker = row['ticker']
        qty = row['quantity']
        cost = row['cost_price']

        # Fetch live price; fall back to cost
        price, _ = fetch_price(ticker)
        if price is None:
            price = cost
            stale_flags.append(True)
        else:
            stale_flags.append(False)

        mv = qty * price
        cost_total = qty * cost
        pnl = mv - cost_total
        pnl_pct = (pnl / cost_total * 100) if cost_total != 0 else 0

        # Daily P&L (from previous close — or cost_price for same-day positions)
        prev_close = get_previous_close(ticker)
        if prev_close and prev_close > 0 and not stale_flags[-1]:
            baseline = prev_close
            # If position was CREATED today, user just opened it;
            # price movement before entry is irrelevant → use cost_price.
            # Note: uses created_at (immutable), NOT updated_at (changes on edit).
            crt = row.get('created_at')
            if crt:
                try:
                    if pd.to_datetime(crt).date() == pd.Timestamp.now().date():
                        baseline = cost
                except Exception:
                    pass
            d_pnl = (price - baseline) * qty
            d_pct = (price / baseline - 1) * 100 if baseline > 0 else 0
        else:
            d_pnl = None
            d_pct = None

        # Regular-close MV (for snapshot baseline — excludes after-hours)
        # prev_close = regularMarketPrice for US stocks when market is closed;
        # for other markets, prev_close ≈ price on non-trading days.
        regular_mvs.append(prev_close * qty if prev_close and prev_close > 0 else mv)

        cur = row['currency']
        rate = fx.get(cur, 1.0)
        mv_cny = mv * rate

        # YTD P&L (per-position, from DB baselines)
        # Method: YTD = current_unrealized − baseline_unrealized
        # This correctly handles qty changes (add/reduce) since baseline.
        if _ytd_baselines:
            bd = _ytd_baselines.get(ticker)  # dict with price, quantity, cost_price
            if bd is not None:
                bp = bd['price']
                b_qty = bd.get('quantity')
                b_cost = bd.get('cost_price')
                if b_qty is not None and b_cost is not None:
                    baseline_unrealized = (bp - b_cost) * b_qty
                    _b_cost_total = b_cost * b_qty   # baseline cost (original ccy)
                else:
                    # Legacy: no qty/cost stored → fall back to price-only
                    baseline_unrealized = (bp - cost) * qty
                    _b_cost_total = cost * qty
                y_pnl = pnl - baseline_unrealized    # current_unrealized − baseline_unrealized
                _avg_cost = (_b_cost_total + cost_total) / 2  # avg of baseline & current cost
                y_pct = (y_pnl / _avg_cost * 100) if _avg_cost != 0 else 0
                ytd_base_costs.append(_b_cost_total * rate)   # baseline cost in CNY
            else:
                # Position opened after baseline → YTD = total P&L, base = entry cost
                y_pnl = pnl
                y_pct = (y_pnl / cost_total * 100) if cost_total != 0 else 0
                ytd_base_costs.append(cost_total * rate)
            ytd_pnls.append(y_pnl)
            ytd_pnl_pcts.append(y_pct)
            ytd_pnl_cnys.append(y_pnl * rate)
        else:
            ytd_pnls.append(None)
            ytd_pnl_pcts.append(None)
            ytd_pnl_cnys.append(None)
            ytd_base_costs.append(None)

        prices.append(price)
        market_values.append(mv)
        market_values_cny.append(mv_cny)
        costs_total.append(cost_total)
        pnls.append(pnl)
        pnl_pcts.append(pnl_pct)
        pnl_cnys.append(pnl * rate)
        daily_pnls.append(d_pnl)
        daily_pnl_pcts.append(d_pct)
        daily_pnl_cnys.append(d_pnl * rate if d_pnl is not None else None)

    df = df.copy()
    df['price'] = prices
    df['market_value'] = market_values
    df['market_value_cny'] = market_values_cny
    df['cost_total'] = costs_total
    df['pnl'] = pnls
    df['pnl_pct'] = pnl_pcts
    df['pnl_cny'] = pnl_cnys
    df['daily_pnl'] = daily_pnls
    df['daily_pnl_pct'] = daily_pnl_pcts
    df['daily_pnl_cny'] = daily_pnl_cnys
    df['ytd_pnl'] = ytd_pnls
    df['ytd_pnl_pct'] = ytd_pnl_pcts
    df['ytd_pnl_cny'] = ytd_pnl_cnys
    df['ytd_base_cost_cny'] = ytd_base_costs
    df['price_stale'] = stale_flags
    df['regular_mv'] = regular_mvs

    # Sector / industry enrichment
    df['sector'] = df['ticker'].map(lambda t: industry_map.get(t, ('', ''))[0])
    df['industry'] = df['ticker'].map(lambda t: industry_map.get(t, ('', ''))[1])

    total_mv = df['market_value_cny'].sum()
    df['weight'] = df['market_value_cny'] / total_mv * 100 if total_mv > 0 else 0

    return df


# ────────────────────────────────────────
# FX Banner
# ────────────────────────────────────────

def render_fx_banner(fx):
    html = '<div class="fx-banner">'
    for label, cur, decimals in [('USD/CNY', 'USD', 4), ('HKD/CNY', 'HKD', 4), ('JPY/CNY', 'JPY', 5)]:
        rate = fx.get(cur)
        val = f'{rate:.{decimals}f}' if rate else '—'
        html += f'<span>{label}: <b>{val}</b></span>'
    html += '<span style="opacity:0.45;font-size:11px;">via Yahoo Finance</span>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ────────────────────────────────────────
# KPI Header
# ────────────────────────────────────────

def render_kpi(df, cash_df, fx, current_capital=None):
    # Total equity market value
    equity_mv = df['market_value_cny'].sum() if not df.empty else 0

    # Cash total in CNY
    cash_cny = 0
    if not cash_df.empty:
        for _, row in cash_df.iterrows():
            rate = fx.get(row['currency'], 1.0)
            cash_cny += row['balance'] * rate

    # Leverage (multi-currency → convert to CNY)
    margin_df = load_margin()
    in_house = 0
    off_exchange = 0
    if not margin_df.empty:
        for _, row in margin_df.iterrows():
            rate = fx.get(row['currency'], 1.0)
            amt_cny = row['amount'] * rate
            if row['category'] == 'in_house':
                in_house += amt_cny
            elif row['category'] == 'off_exchange':
                off_exchange += amt_cny
    total_leverage = in_house + off_exchange

    # Key metrics
    total_assets = equity_mv + cash_cny                     # 资产总值 = 权益 + 现金
    net_assets = total_assets - total_leverage               # 资产净值 = 总值 - 杠杆

    # Total P&L (and per-market MV for snapshot) — vectorised, no iterrows
    import json as _json
    if not df.empty:
        total_pnl_cny = df['pnl_cny'].sum()
        total_cost_cny = (df['market_value_cny'] - df['pnl_cny']).sum()
        _market_mv = df.groupby('market')['market_value_cny'].sum().to_dict()
        # Per-market local-currency MV — live (for JSON blob / display)
        _market_cur_mv = {}
        for (mkt, cur), sub in df.groupby(['market', 'currency']):
            _market_cur_mv.setdefault(mkt, {})[cur] = float(sub['market_value'].sum())
        # Per-market local-currency MV — regular close (for snapshot baseline)
        # Uses prev_close-based regular_mv so snapshot stores EOD close,
        # NOT after-hours/pre-market prices. This ensures correct daily deltas.
        _market_snap_mv = {}
        for (mkt, cur), sub in df.groupby(['market', 'currency']):
            _market_snap_mv.setdefault(mkt, {})[cur] = float(sub['regular_mv'].sum())
    else:
        total_pnl_cny = 0
        total_cost_cny = 0
        _market_mv = {}
        _market_cur_mv = {}
        _market_snap_mv = {}

    pnl_pct = (total_pnl_cny / total_cost_cny * 100) if total_cost_cny != 0 else 0

    # Compute capital (shared formula from db.py) — reuse if already computed
    if current_capital is None:
        with get_conn() as _cap_conn:
            current_capital = compute_capital(_cap_conn, fx)

    # Record daily snapshot — store regular-close MV; skip if any price is stale
    _has_stale = bool(not df.empty and 'price_stale' in df.columns and df['price_stale'].any())
    _record_snapshot(net_assets, equity_mv, cash_cny, total_leverage, total_pnl_cny,
                     market_data_json=_json.dumps(_market_cur_mv, ensure_ascii=False) if _market_cur_mv else None,
                     capital=current_capital,
                     has_stale=_has_stale,
                     market_detail=_market_snap_mv or None)

    # Daily P&L — vectorised from pre-computed daily_pnl_cny column
    daily_pnl = None
    daily_pnl_pct = None
    if not df.empty and 'daily_pnl_cny' in df.columns:
        _dp_mask = df['daily_pnl_cny'].notna()
        if _dp_mask.any():
            _dp_total = df.loc[_dp_mask, 'daily_pnl_cny'].sum()
            _dp_base = (df.loc[_dp_mask, 'market_value_cny'] - df.loc[_dp_mask, 'daily_pnl_cny']).sum()
            daily_pnl = _dp_total
            daily_pnl_pct = (_dp_total / _dp_base * 100) if _dp_base != 0 else 0

    # Weekly P&L from snapshots — uses equity MV change (same metric as strip)
    weekly_pnl = None
    weekly_pnl_pct = None
    weekly_days = None          # actual days since comparison snapshot
    _wk_base_date = None        # shared: base snapshot date (for strip reuse)
    snapshots = load_snapshots()
    if not snapshots.empty:
        _today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
        _week_ago = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
        _past = snapshots[snapshots['date'] <= _today_str]
        if not _past.empty:
            # Find dates that have market detail data (include today — 3/7 snapshot = 3/6 close)
            try:
                with get_conn() as _md_conn:
                    _md_dates = set(r[0] for r in _md_conn.execute(
                        "SELECT DISTINCT date FROM snapshot_market_detail WHERE date <= ?",
                        (_today_str,)
                    ).fetchall())
            except Exception:
                _md_dates = set()

            # Strategy: find snapshot with market detail closest to 7 days ago
            _old_snaps = _past[_past['date'] <= _week_ago]
            if not _old_snaps.empty:
                for _i in range(len(_old_snaps)):
                    _d = _old_snaps.iloc[_i]['date']
                    if _d in _md_dates:
                        _wk_base_date = _d
                        break
            # Fallback: oldest past snapshot with market detail
            if _wk_base_date is None:
                for _i in range(len(_past) - 1, -1, -1):
                    _d = _past.iloc[_i]['date']
                    if _d in _md_dates:
                        _wk_base_date = _d
                        break
            if _wk_base_date and _market_mv:
                # Convert snapshot to CNY at current FX (FX-neutral comparison)
                _base_mv = _resolve_snapshot_mv(_wk_base_date, fx)
                _cur_total = sum(_market_mv.values())
                _base_total = sum(_base_mv.values())
                if _base_total > 0:
                    weekly_pnl = _cur_total - _base_total
                    weekly_pnl_pct = weekly_pnl / _base_total * 100
                    weekly_days = (pd.Timestamp.now() - pd.Timestamp(_wk_base_date)).days

    # YTD from snapshot: earliest snapshot with market_detail in current year
    # (includes today — the 3/7 snapshot records 3/6 closing prices and is valid as baseline)
    ytd_return = None
    ytd_pnl = None
    _ytd_base_date = None
    _current_year = pd.Timestamp.now().strftime('%Y')
    try:
        with get_conn() as _ytd_conn:
            _ytd_row = _ytd_conn.execute(
                "SELECT MIN(date) as d FROM snapshot_market_detail WHERE date >= ?",
                (f'{_current_year}-01-01',)
            ).fetchone()
            if _ytd_row and _ytd_row['d']:
                _ytd_base_date = _ytd_row['d']
    except Exception:
        pass

    if _ytd_base_date and _market_mv:
        _ytd_base_mv = _resolve_snapshot_mv(_ytd_base_date, fx)
        _ytd_cur_total = sum(_market_mv.values())
        _ytd_base_total = sum(_ytd_base_mv.values())
        if _ytd_base_total > 0:
            ytd_pnl = _ytd_cur_total - _ytd_base_total
            ytd_return = ytd_pnl / ((_ytd_base_total + _ytd_cur_total) / 2) * 100

    # ── Row 1: Asset overview ──
    pnl_cls = _pnl_class(total_pnl_cny)

    # Asset class weights (% of Net Assets — mirrors broker view)
    # Split equity into Stock vs ETF
    stock_mv = 0
    etf_mv = 0
    if not df.empty and 'sector' in df.columns:
        etf_mask = df['sector'] == 'ETF'
        etf_mv = df.loc[etf_mask, 'market_value_cny'].sum()
        stock_mv = df.loc[~etf_mask, 'market_value_cny'].sum()
    else:
        stock_mv = equity_mv

    # Net cash = cash - leverage (can be negative when leveraged)
    net_cash = cash_cny - total_leverage

    stock_pct = (stock_mv / net_assets * 100) if net_assets > 0 else 0
    etf_pct = (etf_mv / net_assets * 100) if net_assets > 0 else 0
    cash_pct = (net_cash / net_assets * 100) if net_assets > 0 else 0

    html = '<div class="kpi-row">'
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Total Assets</div>
        <div class="kpi-value">¥{_fmt(total_assets)}</div>
        <div class="kpi-sub" style="color:var(--pf-text2);">Equity + Cash</div>
    </div>'''
    cash_color = 'var(--pf-accent)' if net_cash >= 0 else '#f85149'
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Net Assets</div>
        <div class="kpi-value">¥{_fmt(net_assets)}</div>
        <div class="kpi-sub" style="color:var(--pf-text2);">
            <span style="color:var(--pf-accent);">Stock {stock_pct:.0f}%</span>
            {f' · <span style="color:#06b6d4;">ETF {etf_pct:.0f}%</span>' if etf_mv > 0 else ''}
            · <span style="color:{cash_color};">Cash {cash_pct:.0f}%</span>
        </div>
    </div>'''
    if total_leverage > 0:
        html += f'''<div class="kpi-card">
            <div class="kpi-label">Leverage</div>
            <div class="kpi-value kpi-red">¥{_fmt(total_leverage)}</div>
            <div class="kpi-sub" style="color:var(--pf-text2);">In: ¥{_fmt(in_house)} · Off: ¥{_fmt(off_exchange)}</div>
        </div>'''
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Cash</div>
        <div class="kpi-value">¥{_fmt(cash_cny)}</div>
    </div>'''
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Positions</div>
        <div class="kpi-value">{len(df)}</div>
        <div class="kpi-sub" style="color:var(--pf-text2);">{df["market"].nunique() if not df.empty else 0} markets</div>
    </div>'''
    html += '</div>'

    # ── Row 2: P&L metrics ──
    html += '<div class="kpi-row">'
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Unrealized P&L</div>
        <div class="kpi-value {pnl_cls}">{_pnl_sign(total_pnl_cny)}</div>
        <div class="kpi-sub {pnl_cls}">{_pnl_sign(pnl_pct, 1)}%</div>
    </div>'''

    # Realised P&L from closed trades (use pre-converted CNY column)
    closed_df = load_closed()
    realized_pnl_cny = 0
    if not closed_df.empty:
        realized_pnl_cny = closed_df['realized_pnl_cny'].sum()
    rp_cls = _pnl_class(realized_pnl_cny)
    html += f'''<div class="kpi-card">
        <div class="kpi-label">Realised P&L</div>
        <div class="kpi-value {rp_cls}">{_pnl_sign(realized_pnl_cny)}</div>
        <div class="kpi-sub" style="color:var(--pf-text2);">{len(closed_df)} trades</div>
    </div>'''

    if daily_pnl is not None:
        dp_cls = _pnl_class(daily_pnl)
        html += f'''<div class="kpi-card">
            <div class="kpi-label">Daily P&L</div>
            <div class="kpi-value {dp_cls}">{_pnl_sign(daily_pnl)}</div>
            <div class="kpi-sub {dp_cls}">{_pnl_sign(daily_pnl_pct, 2)}%</div>
        </div>'''

    # Fallback: if no weekly snapshot available, use daily P&L as "Last 1d"
    if weekly_pnl is None and daily_pnl is not None:
        weekly_pnl = daily_pnl
        weekly_pnl_pct = daily_pnl_pct
        weekly_days = 1

    if weekly_pnl is not None:
        wp_cls = _pnl_class(weekly_pnl)
        wk_label = f'Last 7 Days' if not weekly_days or weekly_days >= 7 else f'Last {weekly_days}d'
        html += f'''<div class="kpi-card">
            <div class="kpi-label">{wk_label}</div>
            <div class="kpi-value {wp_cls}">{_pnl_sign(weekly_pnl)}</div>
            <div class="kpi-sub {wp_cls}">{_pnl_sign(weekly_pnl_pct, 2)}%</div>
        </div>'''

    if ytd_return is not None:
        ytd_cls = _pnl_class(ytd_return)
        html += f'''<div class="kpi-card">
            <div class="kpi-label">YTD Return</div>
            <div class="kpi-value {ytd_cls}">{_pnl_sign(ytd_pnl)}</div>
            <div class="kpi-sub {ytd_cls}">{_pnl_sign(ytd_return, 2)}%</div>
            <div style="font-size:9px;color:var(--pf-text2);opacity:0.6;">vs. 3/6 Close</div>
        </div>'''

    html += '</div>'

    # ── Per-market P&L strips (Daily / Weekly) ──
    _MARKET_ORDER = ['美股', '港股', 'A股', 'B股', '日股', '基金']
    def _build_strip(label, items_dict, base_dict=None):
        """Build a per-market P&L strip row (label + pills). Returns (label_html, pills_html)."""
        if not items_dict:
            return ('', '')
        sorted_m = sorted(items_dict.keys(),
                          key=lambda m: _MARKET_ORDER.index(m) if m in _MARKET_ORDER else 99)
        pills = []
        for m in sorted_m:
            chg = items_dict[m]
            base = (base_dict or {}).get(m, 0)
            pct = (chg / base * 100) if base != 0 else 0
            color = 'var(--pf-green)' if chg >= 0 else 'var(--pf-red)'
            sign = '+' if chg >= 0 else ''
            pills.append(
                f'<span style="color:{color};">{m} {sign}{_fmt(chg)}'
                f'<span style="opacity:0.7;">({sign}{pct:.1f}%)</span></span>'
            )
        return (label, ' · '.join(pills))

    def _snap_strip(label, cur_mv, prev_date):
        """Per-market strip from snapshot comparison (FX-neutral: converts at current FX)."""
        if not prev_date or not cur_mv:
            return ('', '')
        prev_mv = _resolve_snapshot_mv(prev_date, fx)
        if not prev_mv:
            return ('', '')
        items = {}
        bases = {}
        for m in set(list(cur_mv.keys()) + list(prev_mv.keys())):
            items[m] = cur_mv.get(m, 0) - prev_mv.get(m, 0)
            bases[m] = prev_mv.get(m, 0)
        return _build_strip(label, items, bases)

    strip_rows = []   # list of (label, pills_html)

    # Daily per-market — vectorised from pre-computed daily_pnl_cny column
    day_by_mkt = {}
    day_base_mkt = {}
    if not df.empty and 'daily_pnl_cny' in df.columns:
        _dp_df = df[df['daily_pnl_cny'].notna()]
        if not _dp_df.empty:
            day_by_mkt = _dp_df.groupby('market')['daily_pnl_cny'].sum().to_dict()
            _prev_mv = _dp_df['market_value_cny'] - _dp_df['daily_pnl_cny']
            day_base_mkt = _dp_df.assign(_prev=_prev_mv).groupby('market')['_prev'].sum().to_dict()
    if day_by_mkt:
        strip_rows.append(_build_strip('Daily', day_by_mkt, day_base_mkt))

    # Weekly per-market — reuse the same base snapshot found for KPI card
    if _wk_base_date and _market_mv:
        _wk_label = f'Last 7 Days' if not weekly_days or weekly_days >= 7 else f'Last {weekly_days}d'
        strip_rows.append(_snap_strip(_wk_label, _market_mv, _wk_base_date))
    elif not _wk_base_date and day_by_mkt:
        strip_rows.append(_build_strip('Last 1d', day_by_mkt, day_base_mkt))

    # YTD per-market strip (snapshot-based, same baseline as KPI card)
    if _ytd_base_date and _market_mv:
        strip_rows.append(_snap_strip('YTD', _market_mv, _ytd_base_date))

    # Render strips as aligned grid: label | per-market details
    strip_rows = [(l, p) for l, p in strip_rows if l and p]
    if strip_rows:
        grid_html = (
            '<div style="display:grid;grid-template-columns:auto 1fr;gap:1px 10px;'
            'font-family:var(--pf-mono);font-size:12px;color:var(--pf-text2);'
            'margin-top:4px;margin-bottom:8px;align-items:baseline;">'
        )
        for lbl, pills in strip_rows:
            grid_html += f'<span style="opacity:0.6;white-space:nowrap;">{lbl}</span>'
            grid_html += f'<span>{pills}</span>'
        grid_html += '</div>'
        html += grid_html

    st.markdown(html, unsafe_allow_html=True)

    # Stale price warning + auto-retry (up to 3 attempts, 5 min apart)
    _MAX_RETRIES = 3
    _RETRY_INTERVAL_MS = 300_000  # 5 minutes
    if _has_stale:
        _stale_tickers = list(df.loc[df['price_stale'], 'ticker']) if 'price_stale' in df.columns else []
        _retry_count = st.session_state.get('_price_retry_count', 0)

        if _retry_count < _MAX_RETRIES:
            _remaining = _MAX_RETRIES - _retry_count
            st.warning(
                f"⚠️ 以下标的取价失败（快照未记录）: {', '.join(_stale_tickers)}  \n"
                f"将在 5 分钟后自动重试（剩余 {_remaining} 次）"
            )
            st.session_state['_price_retry_count'] = _retry_count + 1
            build_portfolio.clear()
            st.markdown(
                f'<script>setTimeout(function(){{window.location.reload()}},{_RETRY_INTERVAL_MS})</script>',
                unsafe_allow_html=True,
            )
        else:
            st.error(
                f"⚠️ 以下标的经 {_MAX_RETRIES} 次重试仍取价失败: {', '.join(_stale_tickers)}  \n"
                "今日快照未记录，请稍后手动刷新。"
            )
    else:
        # Prices all OK — reset retry counter
        if st.session_state.get('_price_retry_count', 0) > 0:
            st.session_state['_price_retry_count'] = 0


def _resolve_snapshot_mv(snap_date, fx):
    """Load per-market MV from snapshot_market_detail table, convert to CNY at given FX.

    Returns {market: cny_value} dict.
    Old snapshots (backfilled with currency='CNY') are handled correctly — the
    backfill migration stores old CNY values as-is, so they pass through unchanged.
    """
    result = {}
    try:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT market, currency, mv FROM snapshot_market_detail WHERE date = ?",
                (snap_date,)
            ).fetchall()
        for row in rows:
            market, cur, mv = row['market'], row['currency'], row['mv']
            result[market] = result.get(market, 0) + mv * fx.get(cur, 1.0)
    except Exception:
        pass
    return result


_ytd_baseline_confirmed = {}   # {year: True} — skip DB check once confirmed


def _record_snapshot(net_assets, equity_mv, cash_cny, leverage_cny, total_pnl_cny,
                     market_data_json=None, capital=None, has_stale=False, market_detail=None):
    """Auto-record today's portfolio snapshot.

    Rules:
      1. Only record after 06:00 Beijing time (stable baseline = all markets' previous close).
      2. Skip if any prices are stale (incomplete data).
      3. First write of the day wins (INSERT OR IGNORE in db.py).
    Also auto-records YTD baseline prices on the first snapshot of a new year.
    """
    if has_stale:
        return  # Don't persist snapshots with unreliable price data

    # Gate: only record after 06:00 local time
    now = pd.Timestamp.now()
    if now.hour < 6:
        return

    # Skip Sunday — no markets trade on Sunday.
    # Saturday is kept: US markets close Friday 4PM ET = Saturday ~4AM Beijing time,
    # so the Saturday snapshot captures Friday's closing prices.
    if now.dayofweek == 6:  # Sunday
        return

    today = now.strftime('%Y-%m-%d')
    total_assets = equity_mv + cash_cny  # 资产总值 = 权益 + 现金
    try:
        with get_conn() as conn:
            upsert_snapshot(conn, today, total_assets, net_assets, equity_mv, cash_cny, leverage_cny,
                            total_pnl_cny, market_data=market_data_json, capital=capital,
                            market_detail=market_detail)

            # Auto-record YTD baselines on first snapshot of a new year.
            # Uses prev_close (regular session close) as baseline price.
            # Memory flag avoids repeated DB checks after first confirmation.
            _year = now.year
            if _year not in _ytd_baseline_confirmed:
                _existing = conn.execute(
                    "SELECT COUNT(*) FROM ytd_baseline_prices WHERE year = ?", (_year,)
                ).fetchone()[0]
                if _existing > 0:
                    _ytd_baseline_confirmed[_year] = True
                else:
                    _bp = {}
                    for row in conn.execute(
                        "SELECT ticker, currency, quantity, cost_price "
                        "FROM positions WHERE status='open' AND ticker != ''"
                    ).fetchall():
                        _tk = row['ticker'] if isinstance(row, sqlite3.Row) else row[0]
                        _cur = row['currency'] if isinstance(row, sqlite3.Row) else row[1]
                        _qty = row['quantity'] if isinstance(row, sqlite3.Row) else row[2]
                        _cp = row['cost_price'] if isinstance(row, sqlite3.Row) else row[3]
                        _pc = get_previous_close(_tk)
                        if _pc and _pc > 0:
                            _bp[_tk] = (_pc, _cur, _qty, _cp)
                    if _bp:
                        record_ytd_baselines(conn, _year, _bp, today)
                        _ytd_baseline_confirmed[_year] = True

            conn.commit()
    except Exception as e:
        import traceback as _tb
        print(f"[snapshot] Error recording snapshot: {e}\n{_tb.format_exc()}", flush=True)


# ────────────────────────────────────────
# Asset Allocation Charts
# ────────────────────────────────────────

_MARKET_COLORS = {
    'A股': '#3b82f6', 'B股': '#6366f1', '港股': '#f59e0b',
    '美股': '#10b981', '日股': '#ef4444', '基金': '#8b5cf6',
}
_CURRENCY_COLORS = {'CNY': '#3b82f6', 'USD': '#10b981', 'HKD': '#f59e0b', 'JPY': '#ef4444'}
_SECTOR_COLORS = {
    'Technology': '#3b82f6', 'Consumer Cyclical': '#f59e0b',
    'Consumer Defensive': '#10b981', 'Communication Services': '#8b5cf6',
    'Industrials': '#6366f1', 'Healthcare': '#ec4899',
    'Energy': '#ef4444', 'Basic Materials': '#14b8a6',
    'Financial Services': '#f97316', 'Real Estate': '#84cc16',
    'Utilities': '#a855f7', 'ETF': '#06b6d4',
}
_SECTOR_SHORT = {
    'Consumer Cyclical': 'Cons.Cycl',
    'Consumer Defensive': 'Cons.Def',
    'Communication Services': 'Comm.Svc',
    'Financial Services': 'Financial',
    'Basic Materials': 'Materials',
    'Real Estate': 'Real Est.',
}


def _donut(labels, values, colors, title):
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.55, textinfo='label+percent', textposition='outside',
        marker=dict(colors=colors, line=dict(width=1, color='rgba(0,0,0,0.1)')),
        textfont=dict(size=11, family='SF Mono, Consolas, monospace'),
        hovertemplate='%{label}<br>¥%{value:,.0f}<br>%{percent}<extra></extra>',
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color='#8b949e'), x=0.5),
        showlegend=False, height=300, margin=dict(t=40, b=20, l=20, r=20),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    )
    return fig


def render_allocation(df):
    if df.empty:
        return

    st.markdown('<div class="section-title">Asset Allocation</div>', unsafe_allow_html=True)

    # Check if we have sector data
    has_sector = not df.empty and df['sector'].str.len().sum() > 0

    if has_sector:
        col1, col2, col3 = st.columns(3)
    else:
        col1, col2 = st.columns(2)

    # By market
    by_market = df.groupby('market')['market_value_cny'].sum().sort_values(ascending=False)
    colors = [_MARKET_COLORS.get(m, '#6b7280') for m in by_market.index]
    col1.plotly_chart(_donut(by_market.index, by_market.values, colors, 'By Market'),
                      use_container_width=True)

    # By currency
    by_currency = df.groupby('currency')['market_value_cny'].sum().sort_values(ascending=False)
    colors = [_CURRENCY_COLORS.get(c, '#6b7280') for c in by_currency.index]
    col2.plotly_chart(_donut(by_currency.index, by_currency.values, colors, 'By Currency'),
                      use_container_width=True)

    # By sector (only if FMP data available)
    if has_sector:
        by_sector = df[df['sector'] != ''].groupby('sector')['market_value_cny'].sum().sort_values(ascending=False)
        if not by_sector.empty:
            short_labels = [_SECTOR_SHORT.get(s, s) for s in by_sector.index]
            colors = [_SECTOR_COLORS.get(s, '#6b7280') for s in by_sector.index]
            col3.plotly_chart(_donut(short_labels, by_sector.values, colors, 'By Sector'),
                              use_container_width=True)


# ────────────────────────────────────────
# Holdings Table
# ────────────────────────────────────────

@st.fragment
def render_holdings(df, fx):
    if df.empty:
        st.info("No positions found.")
        return

    # Find most recent manual update
    latest_update = None
    if not df.empty and 'updated_at' in df.columns:
        latest_update = df['updated_at'].max()

    # Determine price data sources present in portfolio
    _has_fund = False
    _has_stock = False
    if not df.empty:
        import re as _re
        for _tk in df['ticker']:
            if _re.match(r'^\d{6}$', _tk):
                _has_fund = True
            else:
                _has_stock = True
    _src_parts = []
    if _has_stock:
        _src_parts.append('Yahoo Finance')
    if _has_fund:
        _src_parts.append('天天基金')
    _src_label = ' · '.join(_src_parts) if _src_parts else ''

    holdings_header = '<div class="section-title">Holdings'
    if latest_update:
        holdings_header += (
            f' <span style="font-size:11px;font-weight:400;color:var(--pf-text2);'
            f'text-transform:none;letter-spacing:0;">— last updated {latest_update}</span>'
        )
    if _src_label:
        holdings_header += (
            f' <span style="font-size:11px;font-weight:400;color:var(--pf-text2);'
            f'text-transform:none;letter-spacing:0;opacity:0.6;">· prices via {_src_label}</span>'
        )
    holdings_header += '</div>'
    st.markdown(holdings_header, unsafe_allow_html=True)

    # Filters — Search (text input), Market, Broker, Sector, Sort
    col_search, col_market, col_broker, col_sector, col_sort = st.columns([2, 1, 1, 1, 1])
    search_text = col_search.text_input(
        'Search', placeholder='🔍  Name / Ticker',
        key='holdings_search',
    )
    markets = ['All'] + sorted(df['market'].unique().tolist())
    sel_market = col_market.selectbox('Market', markets)
    brokers = ['All'] + sorted(df['broker'].unique().tolist())
    sel_broker = col_broker.selectbox('Broker', brokers)
    sectors_list = sorted(df[df['sector'].str.len() > 0]['sector'].unique().tolist())
    sectors = ['All'] + sectors_list
    sel_sector = col_sector.selectbox('Sector', sectors)
    sort_by = col_sort.selectbox('Sort', ['MV ↓', 'Total P&L ↓', 'Total P&L ↑',
                                           'Daily P&L ↓', 'Daily P&L ↑',
                                           'YTD P&L ↓', 'YTD P&L ↑', 'Weight ↓'])

    filtered = df.copy()
    if search_text:
        q = search_text.strip().lower()
        filtered = filtered[
            filtered['name'].str.lower().str.contains(q, na=False)
            | filtered['ticker'].str.lower().str.contains(q, na=False)
        ]
    if sel_market != 'All':
        filtered = filtered[filtered['market'] == sel_market]
    if sel_broker != 'All':
        filtered = filtered[filtered['broker'] == sel_broker]
    if sel_sector != 'All':
        filtered = filtered[filtered['sector'] == sel_sector]

    sort_map = {
        'MV ↓': ('market_value_cny', False),
        'Total P&L ↓': ('pnl_cny', False),
        'Total P&L ↑': ('pnl_cny', True),
        'Daily P&L ↓': ('daily_pnl_cny', False),
        'Daily P&L ↑': ('daily_pnl_cny', True),
        'YTD P&L ↓': ('ytd_pnl_cny', False),
        'YTD P&L ↑': ('ytd_pnl_cny', True),
        'Weight ↓': ('weight', False),
    }
    sort_col, sort_asc = sort_map.get(sort_by, ('market_value_cny', False))
    filtered = filtered.sort_values(sort_col, ascending=sort_asc, na_position='last').reset_index(drop=True)

    has_industry = df['industry'].str.len().sum() > 0

    # ── Frozen column left offsets (cumulative widths) ──
    _freeze_cols = {
        'Name':   ('left:0px', 140),
        'Ticker': ('left:140px', 95),
        'Market': ('left:235px', 55),
        'Broker': ('left:290px', 60),
    }
    _freeze_end_col = 'Broker'  # last frozen column gets shadow

    def _fc(col_name, is_last=False, extra_cls=''):
        """Return class+style attributes for a frozen header <th>."""
        classes = []
        if col_name in _freeze_cols:
            classes.append('frozen')
            if is_last:
                classes.append('freeze-end')
        if extra_cls:
            classes.append(extra_cls)
        if not classes and col_name not in _freeze_cols:
            return ''
        pos, _ = _freeze_cols.get(col_name, ('', 0))
        cls_str = f' class="{" ".join(classes)}"' if classes else ''
        sty_str = f' style="{pos}"' if pos else ''
        return cls_str + sty_str

    def _fctd(col_name, extra_cls='', is_last=False):
        """Return class + style for a frozen <td>."""
        classes = []
        styles = []
        if col_name in _freeze_cols:
            pos, _ = _freeze_cols[col_name]
            classes.append('frozen')
            if is_last:
                classes.append('freeze-end')
            styles.append(pos)
        if extra_cls:
            classes.append(extra_cls)
        cls_str = f' class="{" ".join(classes)}"' if classes else ''
        sty_str = f' style="{";".join(styles)}"' if styles else ''
        return cls_str + sty_str

    def _pnl_td(val, decimals=0, is_pct=False):
        """Format a P&L value as a colored <td class="num">."""
        if val is None or pd.isna(val):
            return '<td class="num">—</td>'
        cls = 'pnl-pos' if val >= 0 else 'pnl-neg'
        sign = '+' if val > 0 else ''
        if is_pct:
            txt = f'{sign}{val:,.{decimals}f}%'
        else:
            txt = f'{sign}{val:,.{decimals}f}'
        return f'<td class="num {cls}">{txt}</td>'

    # ── Build HTML table ──
    # Embed critical CSS so it's always co-located (survives fragment reruns)
    html = '''<style>
.holdings-wrap{overflow:auto;max-height:82vh;max-width:100%;border:1px solid var(--pf-border);border-radius:8px;margin-bottom:12px}
.holdings-table{width:max-content;min-width:100%;border-collapse:separate;border-spacing:0;font-family:var(--pf-mono);font-size:13px}
.holdings-table th{text-align:left;padding:6px 10px;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:var(--pf-text2);border-bottom:1px solid var(--pf-border);background:var(--pf-bg);white-space:nowrap;position:sticky;top:0;z-index:5}
.holdings-table th.num{text-align:right}
.holdings-table th.sub{font-size:10px;font-weight:400;text-transform:none;letter-spacing:0;padding-top:0;border-bottom:2px solid var(--pf-border)}
.holdings-table td{padding:5px 10px;border-bottom:1px solid var(--pf-border);color:var(--pf-text);white-space:nowrap;background:var(--pf-bg)}
.holdings-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.holdings-table .ind-text{display:inline-block;max-width:80px;overflow:hidden;text-overflow:ellipsis;vertical-align:bottom}
.holdings-table .after-freeze{padding-left:16px}
.holdings-table tbody tr:hover td{background:color-mix(in srgb,var(--pf-accent) 5%,var(--pf-bg))}
.holdings-table .frozen{position:sticky;z-index:2}
.holdings-table thead .frozen{z-index:6}
.holdings-table .freeze-end{box-shadow:2px 0 4px rgba(0,0,0,.05)}
.holdings-table tbody tr:last-child td{border-bottom:none}
.holdings-table tfoot td{border-top:2px solid var(--pf-border);border-bottom:none;font-weight:600;padding:8px 10px}
.holdings-table .row-link{color:var(--pf-accent);cursor:pointer;font:inherit;font-family:var(--pf-mono);font-size:13px}
.holdings-table .row-link:hover{text-decoration:underline}
.holdings-table td.pnl-pos,.holdings-table tfoot td.pnl-pos{color:var(--pf-green)!important}
.holdings-table td.pnl-neg,.holdings-table tfoot td.pnl-neg{color:var(--pf-red)!important}
</style>'''
    html += '<div class="holdings-wrap"><table class="holdings-table">'

    # Header row 1 — column names
    html += '<thead><tr>'
    html += f'<th{_fc("Name")}>Name</th>'
    html += f'<th{_fc("Ticker")}>Ticker</th>'
    html += f'<th{_fc("Market")}>Market</th>'
    html += f'<th{_fc("Broker", is_last=True)}>Broker</th>'
    html += '<th class="after-freeze">Currency</th>'
    if has_industry:
        html += '<th>Industry</th>'
    html += '<th class="num">Qty</th>'
    html += '<th class="num">Cost</th>'
    html += '<th class="num">Price</th>'
    html += '<th class="num">MV</th>'
    html += '<th class="num">MV(¥)</th>'
    html += '<th class="num">Daily P&L</th>'
    html += '<th class="num">Daily%</th>'
    html += '<th class="num" title="Baseline: 2026-03-06 Close">YTD P&L</th>'
    html += '<th class="num" title="Baseline: 2026-03-06 Close · Denominator: avg(baseline cost, current cost)">YTD P&L%</th>'
    html += '<th class="num">Total P&L</th>'
    html += '<th class="num">Total P&L %</th>'
    html += '<th class="num">Wt%</th>'
    html += '</tr>'

    # Header row 2 — currency / sub-labels
    def _sub_th(col_name, text='', num=False, is_last=False):
        """Build a <th class="sub ..."> with optional frozen + num."""
        classes = ['sub']
        style = ''
        if col_name in _freeze_cols:
            classes.append('frozen')
            if is_last:
                classes.append('freeze-end')
            pos, _ = _freeze_cols[col_name]
            style = f' style="{pos}"'
        if num:
            classes.append('num')
        return f'<th class="{" ".join(classes)}"{style}>{text}</th>'

    html += '<tr>'
    html += _sub_th('Name', '')
    html += _sub_th('Ticker', '')
    html += _sub_th('Market', '')
    html += _sub_th('Broker', '', is_last=True)
    html += '<th class="sub after-freeze"></th>'
    if has_industry:
        html += '<th class="sub"></th>'
    html += '<th class="sub num"></th>'
    html += '<th class="sub num">original</th>'
    html += '<th class="sub num">original</th>'
    html += '<th class="sub num">original</th>'
    html += '<th class="sub num">CNY</th>'
    html += '<th class="sub num">CNY</th>'
    html += '<th class="sub num"></th>'
    html += '<th class="sub num">CNY</th>'
    html += '<th class="sub num"></th>'
    html += '<th class="sub num">CNY</th>'
    html += '<th class="sub num"></th>'
    html += '<th class="sub num"></th>'
    html += '</tr></thead>'

    # Build sidebar-index lookup: (ticker, broker) → selectbox index
    _pos_df = load_positions()
    _sidebar_idx = {}
    for _i, (_, _p) in enumerate(_pos_df.iterrows()):
        _sidebar_idx[(_p['ticker'], _p['broker'])] = _i

    # Body rows
    html += '<tbody>'
    t_mv_cny = t_pnl_cny = t_dp_cny = t_dp_base = 0.0
    t_ytd_cny = t_ytd_base_cost = t_ytd_cur_cost = 0.0
    t_wt = 0.0
    for _, r in filtered.iterrows():
        pnl_cny = r['pnl_cny']
        dp_cny = r.get('daily_pnl_cny')
        has_dp = dp_cny is not None and not pd.isna(dp_cny)
        dpct = r.get('daily_pnl_pct')
        qty = int(r['quantity']) if r['quantity'] == int(r['quantity']) else r['quantity']

        # Accumulators for totals
        t_mv_cny += r['market_value_cny']
        t_pnl_cny += pnl_cny
        if has_dp:
            t_dp_cny += dp_cny
            t_dp_base += r['market_value_cny'] - dp_cny
        _ytd_c = r.get('ytd_pnl_cny')
        if _ytd_c is not None and not pd.isna(_ytd_c):
            t_ytd_cny += _ytd_c
            t_ytd_cur_cost += r['market_value_cny'] - r['pnl_cny']  # current cost in CNY
        _ytd_bc = r.get('ytd_base_cost_cny')
        if _ytd_bc is not None and not pd.isna(_ytd_bc):
            t_ytd_base_cost += _ytd_bc
        t_wt += r['weight']

        # Click-to-edit: Name cell is a clickable span (handled by JS below)
        _sidx = _sidebar_idx.get((r['ticker'], r['broker']))
        if _sidx is not None:
            _name_html = f'<span class="row-link" data-idx="{_sidx}">{r["name"]}</span>'
        else:
            _name_html = r['name']

        # Red dot: position was updated today (cost/qty corrected)
        _red_dot = ''
        _upd = r.get('updated_at', '')
        if _upd:
            try:
                _upd_dt = pd.to_datetime(_upd)
                if _upd_dt.date() == pd.Timestamp.now().date():
                    _tip = f'{_upd_dt.strftime("%Y-%-m-%-d %H:%M")}'
                    _red_dot = f'<span class="red-dot" data-tip="{_tip}">●</span>'
            except Exception:
                pass

        html += '<tr>'
        html += f'<td{_fctd("Name")}>{_name_html}{_red_dot}</td>'
        html += f'<td{_fctd("Ticker")}>{r["ticker"]}</td>'
        html += f'<td{_fctd("Market")}>{r["market"]}</td>'
        html += f'<td{_fctd("Broker", is_last=True)}>{r["broker"]}</td>'
        html += f'<td class="after-freeze">{r["currency"]}</td>'
        if has_industry:
            _ind = r.get("industry", "")
            html += f'<td title="{_ind}"><span class="ind-text">{_ind}</span></td>'
        html += f'<td class="num">{qty:,}</td>'
        html += f'<td class="num">{r["cost_price"]:,.2f}</td>'
        html += f'<td class="num">{r["price"]:,.2f}</td>'
        html += f'<td class="num">{r["market_value"]:,.0f}</td>'
        html += f'<td class="num">{r["market_value_cny"]:,.0f}</td>'
        html += _pnl_td(dp_cny)
        html += _pnl_td(dpct, decimals=1, is_pct=True)
        html += _pnl_td(r.get('ytd_pnl_cny'))
        html += _pnl_td(r.get('ytd_pnl_pct'), decimals=1, is_pct=True)
        html += _pnl_td(pnl_cny)
        html += _pnl_td(r['pnl_pct'], decimals=1, is_pct=True)
        html += f'<td class="num">{r["weight"]:.1f}%</td>'
        html += '</tr>'

    html += '</tbody>'

    # Totals row — vectorised
    dp_pct_total = (t_dp_cny / t_dp_base * 100) if t_dp_base else 0
    t_cost_cny = (filtered['market_value_cny'] - filtered['pnl_cny']).sum() if not filtered.empty else 0.0
    pnl_pct_total = (t_pnl_cny / t_cost_cny * 100) if t_cost_cny else 0
    dp_cls = _pnl_class(t_dp_cny)
    pnl_cls = _pnl_class(t_pnl_cny)

    html += '<tfoot><tr>'
    html += f'<td{_fctd("Name")}><b>Total</b></td>'
    html += f'<td{_fctd("Ticker")}></td>'
    html += f'<td{_fctd("Market")}></td>'
    html += f'<td{_fctd("Broker", is_last=True)}></td>'
    html += '<td class="after-freeze"></td>'  # Ccy
    if has_industry:
        html += '<td></td>'
    html += '<td class="num"></td>'  # Qty
    html += '<td class="num"></td>'  # Price
    html += '<td class="num"></td>'  # Cost
    html += '<td class="num"></td>'  # MV local
    html += f'<td class="num"><b>{t_mv_cny:,.0f}</b></td>'
    dp_sign = '+' if t_dp_cny > 0 else ''
    html += f'<td class="num {dp_cls}"><b>{dp_sign}{t_dp_cny:,.0f}</b></td>'
    dpp_sign = '+' if dp_pct_total > 0 else ''
    html += f'<td class="num {dp_cls}"><b>{dpp_sign}{dp_pct_total:.1f}%</b></td>'
    # YTD totals (before Total P&L)
    _has_ytd = 'ytd_pnl_cny' in filtered.columns and filtered['ytd_pnl_cny'].notna().any()
    if _has_ytd:
        _ytd_avg_cost = (t_ytd_base_cost + t_ytd_cur_cost) / 2
        ytd_pct_total = (t_ytd_cny / _ytd_avg_cost * 100) if _ytd_avg_cost else 0
        ytd_cls = _pnl_class(t_ytd_cny)
        ytd_sign = '+' if t_ytd_cny > 0 else ''
        html += f'<td class="num {ytd_cls}"><b>{ytd_sign}{t_ytd_cny:,.0f}</b></td>'
        ytdp_sign = '+' if ytd_pct_total > 0 else ''
        html += f'<td class="num {ytd_cls}"><b>{ytdp_sign}{ytd_pct_total:.1f}%</b></td>'
    else:
        html += '<td class="num"></td><td class="num"></td>'
    # Total P&L
    pnl_sign = '+' if t_pnl_cny > 0 else ''
    html += f'<td class="num {pnl_cls}"><b>{pnl_sign}{t_pnl_cny:,.0f}</b></td>'
    pp_sign = '+' if pnl_pct_total > 0 else ''
    html += f'<td class="num {pnl_cls}"><b>{pp_sign}{pnl_pct_total:.1f}%</b></td>'
    html += f'<td class="num"><b>{t_wt:.1f}%</b></td>'
    html += '</tr></tfoot>'

    html += '</table></div>'
    # JS: (1) dynamically fix frozen-column left offsets, (2) click-to-edit handlers
    html += '''<script>
requestAnimationFrame(function(){
    var table = document.querySelector('.holdings-table');
    if(!table) return;
    /* ── Fix frozen column left offsets based on actual rendered widths ── */
    var firstRow = table.querySelector('thead tr:first-child');
    if(firstRow){
        var ths = firstRow.children;
        var frozenCount = 4;  /* Name, Ticker, Market, Broker */
        var left = 0;
        var offsets = [];
        for(var i = 0; i < frozenCount && i < ths.length; i++){
            offsets.push(left);
            left += ths[i].getBoundingClientRect().width;
        }
        /* Apply computed offsets to ALL rows (thead, tbody, tfoot) */
        var rows = table.querySelectorAll('tr');
        for(var r = 0; r < rows.length; r++){
            var cells = rows[r].children;
            for(var c = 0; c < frozenCount && c < cells.length; c++){
                cells[c].style.left = offsets[c] + 'px';
            }
        }
    }
    /* ── Sticky thead: set row-2 top offset to row-1 height ── */
    var row1 = table.querySelector('thead tr:first-child');
    var row2 = table.querySelector('thead tr:nth-child(2)');
    if(row1 && row2){
        var h = row1.getBoundingClientRect().height;
        Array.from(row2.children).forEach(function(th){ th.style.top = h + 'px'; });
    }
    /* ── Click-to-edit: stock name → set ?edit_pos query param ── */
    table.querySelectorAll('.row-link').forEach(function(el){
        el.addEventListener('click', function(){
            var idx = el.getAttribute('data-idx');
            var url = new URL(window.location);
            url.searchParams.set('edit_pos', idx);
            window.location.href = url.toString();
        });
    });
});
</script>'''
    st.html(html, unsafe_allow_javascript=True)



# ────────────────────────────────────────
# Unrealized P&L by Market (below Holdings)
# ────────────────────────────────────────

def render_unrealized_market_strip(df, fx):
    """Compact strip showing unrealized P&L broken down by market."""
    if df.empty:
        return
    # Vectorised: use pre-computed pnl_cny column; cost_cny = mv_cny - pnl_cny
    _grp = df.groupby('market').agg(
        _pnl=('pnl_cny', 'sum'),
        _mv=('market_value_cny', 'sum'),
    )
    market_pnl = _grp['_pnl'].to_dict()
    market_cost = (_grp['_mv'] - _grp['_pnl']).to_dict()

    _MKT_ORD = ['美股', '港股', 'A股', 'B股', '日股', '基金']
    sorted_markets = sorted(market_pnl.keys(),
                            key=lambda m: _MKT_ORD.index(m) if m in _MKT_ORD else 99)
    pills = []
    for m in sorted_markets:
        p = market_pnl[m]
        c = market_cost[m]
        pct = (p / c * 100) if c != 0 else 0
        color = 'var(--pf-green)' if p >= 0 else 'var(--pf-red)'
        sign = '+' if p >= 0 else ''
        pills.append(
            f'<span style="color:{color};">{m} {sign}{_fmt(p)}'
            f'<span style="opacity:0.7;">({sign}{pct:.1f}%)</span></span>'
        )
    st.markdown(
        f'<div style="font-family:var(--pf-mono);font-size:12px;'
        f'color:var(--pf-text2);margin-top:-8px;margin-bottom:16px;">'
        f'Unrealized: {" · ".join(pills)}'
        f'</div>',
        unsafe_allow_html=True,
    )


# ────────────────────────────────────────
# Cash Table
# ────────────────────────────────────────

def render_cash(cash_df):
    if cash_df.empty:
        return

    st.markdown('<div class="section-title">Cash Balances</div>', unsafe_allow_html=True)

    # Pivot: rows=account, columns=currency
    pivot = cash_df.pivot_table(index='account', columns='currency', values='balance', fill_value=0)
    # Reorder columns
    col_order = [c for c in ('CNY', 'USD', 'HKD', 'JPY') if c in pivot.columns]
    pivot = pivot[col_order]

    # Filter out zero-only rows
    pivot = pivot[pivot.sum(axis=1) != 0]

    html = '<table class="cash-table"><thead><tr><th>Account</th>'
    for c in col_order:
        html += f'<th>{c}</th>'
    html += '</tr></thead><tbody>'
    for acct, row in pivot.iterrows():
        html += f'<tr><td>{acct}</td>'
        for c in col_order:
            v = row.get(c, 0)
            html += f'<td>{_fmt(v)}</td>' if v != 0 else '<td style="color:var(--pf-text2);">—</td>'
        html += '</tr>'
    html += '</tbody></table>'

    st.markdown(html, unsafe_allow_html=True)


# ────────────────────────────────────────
# NAV Performance Chart
# ────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_benchmark_data(earliest_date_str):
    """Fetch benchmark index data from earliest_date to today (cached).

    Always fetches the full range so that switching time ranges (1Y/2Y/YTD)
    doesn't trigger new API calls — just slices from the cached result.
    NOTE: yfinance is NOT thread-safe (parallel downloads corrupt data),
    so we download sequentially but cache aggressively (1hr TTL).
    """
    benchmarks = {
        'CSI 300': '000300.SS',
        'S&P 500': '^GSPC',
        'Hang Seng': '^HSI',
    }
    results = {}
    try:
        import yfinance as yf
        _end = (pd.Timestamp.now() + pd.DateOffset(days=1)).strftime('%Y-%m-%d')

        for name, ticker in benchmarks.items():
            try:
                hist = yf.download(ticker, start=earliest_date_str, end=_end,
                                   progress=False, auto_adjust=True)
                if hist is not None and not hist.empty:
                    # yfinance returns MultiIndex columns; flatten before rename
                    if isinstance(hist.columns, pd.MultiIndex):
                        hist.columns = hist.columns.droplevel(1)
                    df = hist[['Close']].reset_index()
                    df.columns = ['date', 'close']
                    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                    results[name] = df
            except Exception:
                pass
    except Exception:
        pass
    return results


@st.fragment
def render_performance(current_capital=None):
    nav_df = load_nav()
    snap_df = load_snapshots()

    if nav_df.empty and snap_df.empty:
        return

    # Section header with time range selector and benchmark toggle
    _perf_left, _perf_mid, _perf_right = st.columns([3, 1, 1])
    _perf_left.markdown('<div class="section-title">Performance</div>', unsafe_allow_html=True)
    _show_bench = _perf_mid.checkbox('Benchmarks', value=False, key='perf_bench')
    _range = _perf_right.selectbox(
        'Range', ['7D', 'YTD', '1Y', '2Y', '3Y', 'All'],
        index=3,  # default to 2Y
        label_visibility='collapsed', key='perf_range',
    )

    nav_df = nav_df.copy() if not nav_df.empty else pd.DataFrame(
        columns=['date', 'net_asset_value', 'capital_invested'])
    nav_df['date'] = pd.to_datetime(nav_df['date'])

    # Extend with recent daily_snapshots ONLY after the last nav_history date
    if not snap_df.empty and not nav_df.empty:
        last_nav_date = nav_df['date'].max().strftime('%Y-%m-%d')
        cap = current_capital if current_capital else nav_df.iloc[-1]['capital_invested']
        new_rows = []
        for _, s in snap_df.iterrows():
            if s['date'] > last_nav_date:
                new_rows.append({
                    'date': pd.Timestamp(s['date']),
                    'net_asset_value': s['net_assets'],
                    'capital_invested': cap,
                })
        if new_rows:
            nav_df = pd.concat([nav_df, pd.DataFrame(new_rows)], ignore_index=True)
            nav_df = nav_df.sort_values('date')

    if nav_df.empty:
        return

    # Remember earliest date before range filtering (for benchmark cache key)
    _all_nav_dates_min = nav_df['date'].min()

    # Apply time range filter
    _now = pd.Timestamp.now()
    _range_map = {
        '7D':  _now - pd.DateOffset(days=7),
        'YTD': pd.Timestamp(f'{_now.year}-01-01'),
        '1Y':  _now - pd.DateOffset(years=1),
        '2Y':  _now - pd.DateOffset(years=2),
        '3Y':  _now - pd.DateOffset(years=3),
        'All': pd.Timestamp('2000-01-01'),
    }
    _start = _range_map.get(_range, _range_map['2Y'])
    nav_df = nav_df[nav_df['date'] >= _start]

    if nav_df.empty:
        return

    fig = go.Figure()
    _alpha_html = None

    if _show_bench:
        # ── Normalized return comparison mode ──
        # Use equity_nav (= NAV / Capital) to eliminate capital flow distortions.
        # Raw NAV includes deposits/withdrawals; equity_nav is the true return multiplier.
        nav_df['equity_nav'] = nav_df['net_asset_value'] / nav_df['capital_invested']
        _enav_start = nav_df.iloc[0]['equity_nav']
        nav_df['nav_indexed'] = nav_df['equity_nav'] / _enav_start * 100

        # customdata must be a plain float list for Plotly format specifiers
        _port_ret = (nav_df['nav_indexed'] - 100).round(1).tolist()

        fig.add_trace(go.Scatter(
            x=nav_df['date'], y=nav_df['nav_indexed'],
            name='Portfolio', line=dict(color='#3b82f6', width=2.5),
            hovertemplate='%{x|%Y-%m-%d}<br>Portfolio: %{y:.1f} (%{customdata:+.1f}%)<extra></extra>',
            customdata=_port_ret,
        ))

        # Fetch benchmarks: always from earliest nav date (cache-friendly);
        # slice to current range locally — no re-download on range switch.
        _bench_colors = {'CSI 300': '#ef4444', 'S&P 500': '#22c55e', 'Hang Seng': '#f59e0b'}
        _earliest_str = _all_nav_dates_min.strftime('%Y-%m-%d') if _all_nav_dates_min else nav_df['date'].min().strftime('%Y-%m-%d')
        bench_data = _fetch_benchmark_data(_earliest_str)

        for bname, bdf in bench_data.items():
            # Slice to current range
            bdf = bdf[(bdf['date'] >= nav_df['date'].min()) & (bdf['date'] <= nav_df['date'].max())]
            if bdf.empty:
                continue
            _b_start = bdf.iloc[0]['close']
            if _b_start == 0:
                continue
            bdf = bdf.copy()
            bdf['indexed'] = bdf['close'] / _b_start * 100
            _b_ret = (bdf['indexed'] - 100).round(1).tolist()
            fig.add_trace(go.Scatter(
                x=bdf['date'], y=bdf['indexed'],
                name=bname, line=dict(color=_bench_colors.get(bname, '#8b949e'), width=1.5, dash='dash'),
                hovertemplate='%{x|%Y-%m-%d}<br>' + bname + ': %{y:.1f} (%{customdata:+.1f}%)<extra></extra>',
                customdata=_b_ret,
            ))

        # Compute alpha (excess return) vs each benchmark
        _port_total_ret = nav_df['nav_indexed'].iloc[-1] - 100
        _alpha_parts = []
        for bname, bdf in bench_data.items():
            bdf = bdf[(bdf['date'] >= nav_df['date'].min()) & (bdf['date'] <= nav_df['date'].max())]
            if bdf.empty:
                continue
            _bs = bdf.iloc[0]['close']
            if _bs == 0:
                continue
            _bench_ret = (bdf.iloc[-1]['close'] / _bs - 1) * 100
            _excess = _port_total_ret - _bench_ret
            _color = 'var(--pf-green)' if _excess >= 0 else 'var(--pf-red)'
            _sign = '+' if _excess >= 0 else ''
            _alpha_parts.append(
                f'<span style="margin:0 8px;">vs {bname}: '
                f'<b style="color:{_color};">{_sign}{_excess:.1f}%</b></span>'
            )
        if _alpha_parts:
            _alpha_html = (
                '<div style="font-size:11px;font-family:var(--pf-mono);'
                'color:var(--pf-text2);margin-top:-8px;margin-bottom:24px;">'
                f'Alpha (excess return): {"".join(_alpha_parts)}'
                '</div>'
            )

        _y_title = 'Indexed (100 = start)'
        _y_fmt = None
    else:
        # ── Absolute NAV mode (original) ──
        fig.add_trace(go.Scatter(
            x=nav_df['date'], y=nav_df['net_asset_value'],
            name='Portfolio NAV', line=dict(color='#3b82f6', width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>NAV: ¥%{y:,.0f}<extra></extra>',
        ))
        fig.add_trace(go.Scatter(
            x=nav_df['date'], y=nav_df['capital_invested'],
            name='Capital', line=dict(color='#8b949e', width=1, dash='dot'),
            hovertemplate='%{x|%Y-%m-%d}<br>Capital: ¥%{y:,.0f}<extra></extra>',
        ))
        nav_df['total_pnl'] = nav_df['net_asset_value'] - nav_df['capital_invested']
        fig.add_trace(go.Scatter(
            x=nav_df['date'], y=nav_df['total_pnl'],
            name='Net P&L (NAV−Capital)', line=dict(color='#f59e0b', width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>P&L: ¥%{y:,.0f}<extra></extra>',
        ))
        _y_title = None
        _y_fmt = ','

    fig.update_layout(
        height=350,
        margin=dict(t=10, b=40, l=60, r=60),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, tickfont=dict(size=11, family='SF Mono, Consolas, monospace')),
        yaxis=dict(
            showgrid=True, gridcolor='rgba(128,128,128,0.1)',
            tickfont=dict(size=11, family='SF Mono, Consolas, monospace'),
            tickformat=_y_fmt,
            title=dict(text=_y_title, font=dict(size=11)) if _y_title else None,
        ),
        legend=dict(
            orientation='h', y=1.02, x=0.5, xanchor='center',
            font=dict(size=11, family='SF Mono, Consolas, monospace'),
        ),
        hovermode='x unified',
    )

    st.plotly_chart(fig, use_container_width=True)

    # Show alpha strip when benchmarks are enabled
    if _alpha_html:
        st.markdown(_alpha_html, unsafe_allow_html=True)


# ────────────────────────────────────────
# Risk Analytics
# ────────────────────────────────────────

def render_risk_analytics():
    """Risk metrics using flow-adjusted returns with rolling trend chart.

    Uses total_pnl_cny to compute returns that are independent of capital
    deposits/withdrawals.  Max drawdown is computed from the cumulative
    return index (not raw NAV), so it reflects pure investment performance.

    Formula per period:
        Capital_t = NAV_t − PnL_t
        adj_base  = Capital_t + PnL_{t-1}   (portfolio value after flow, before market move)
        return_t  = (PnL_t − PnL_{t-1}) / adj_base
    """
    snap_df = load_snapshots()
    if snap_df.empty or len(snap_df) < 10:
        return

    snap = snap_df.sort_values('date').copy()
    snap['date_dt'] = pd.to_datetime(snap['date'])
    snap['nav'] = snap['net_assets'].astype(float)
    snap['pnl'] = snap['total_pnl_cny'].astype(float)

    # ── Flow-adjusted period returns ──
    snap['_gap_days'] = snap['date_dt'].diff().dt.days
    snap['pnl_prev'] = snap['pnl'].shift(1)
    snap['adj_base'] = (snap['nav'] - snap['pnl']) + snap['pnl_prev']
    snap['pnl_chg'] = snap['pnl'] - snap['pnl_prev']
    snap = snap.dropna(subset=['pnl_chg', 'adj_base'])
    snap = snap[snap['adj_base'].abs() > 1]
    snap = snap[snap['_gap_days'] <= 90]
    snap['period_ret'] = snap['pnl_chg'] / snap['adj_base']

    if len(snap) < 5:
        return

    # ── Auto-detect frequency ──
    _recent_gaps = snap['_gap_days'].dropna().tail(30)
    _median_gap = _recent_gaps.median() if not _recent_gaps.empty else 7
    if _median_gap <= 2:
        _ppy = 252; _freq_label = '日频'
    elif _median_gap <= 8:
        _ppy = 52; _freq_label = '周频'
    elif _median_gap <= 16:
        _ppy = 26; _freq_label = '双周'
    else:
        _ppy = 12; _freq_label = '月频'

    # ── Cumulative return index & drawdown ──
    snap['cum_ret_idx'] = (1 + snap['period_ret']).cumprod() * 100
    _cum_peak = snap['cum_ret_idx'].cummax()
    snap['drawdown_pct'] = (snap['cum_ret_idx'] - _cum_peak) / _cum_peak * 100  # %

    # ── Rolling metrics ──
    _window = max(_ppy // 4, 8)  # ~3 months of data; at least 8 periods
    _min_p = max(_window // 2, 5)
    _rf = 0.015 / _ppy
    _roll_std = snap['period_ret'].rolling(_window, min_periods=_min_p).std()
    snap['rolling_vol'] = _roll_std * (_ppy ** 0.5) * 100  # annualised %
    snap['rolling_sharpe'] = (
        (snap['period_ret'].rolling(_window, min_periods=_min_p).mean() - _rf) / _roll_std
    ) * (_ppy ** 0.5)

    # ── Whole-period KPI values ──
    returns = snap['period_ret']
    _period_vol = returns.std()
    _annual_vol = _period_vol * (_ppy ** 0.5)
    _max_dd = snap['drawdown_pct'].min() / 100  # as ratio
    _dd_end_idx = snap['drawdown_pct'].idxmin()
    _dd_end_date = snap.loc[_dd_end_idx, 'date']
    _dd_end_pnl = snap.loc[_dd_end_idx, 'pnl']
    _peak_idx = snap.loc[:_dd_end_idx, 'cum_ret_idx'].idxmax()
    _dd_start_date = snap.loc[_peak_idx, 'date']
    _dd_peak_pnl = snap.loc[_peak_idx, 'pnl']
    _dd_pnl_lost = _dd_end_pnl - _dd_peak_pnl
    _sharpe = ((returns.mean() - _rf) / _period_vol) * (_ppy ** 0.5) if _period_vol > 0 else 0
    _win = int((returns > 0).sum())
    _total = len(returns)
    _win_rate = _win / _total * 100
    _total_ret = snap['cum_ret_idx'].iloc[-1] / 100 - 1
    _years = (snap['date_dt'].iloc[-1] - snap['date_dt'].iloc[0]).days / 365.25
    _annual_ret = (1 + _total_ret) ** (1 / _years) - 1 if _years > 0.1 else _total_ret
    _calmar = abs(_annual_ret / _max_dd) if _max_dd != 0 else 0

    # ── Render section ──
    st.markdown('<div class="section-title">Risk Analytics</div>', unsafe_allow_html=True)

    # ── KPI cards ──
    def _risk_card(label, value, sub='', color='var(--pf-text)'):
        return (
            f'<div style="text-align:center;padding:8px 4px;">'
            f'<div style="font-size:11px;color:var(--pf-text2);margin-bottom:2px;">{label}</div>'
            f'<div style="font-size:18px;font-weight:700;font-family:var(--pf-mono);color:{color};">{value}</div>'
            f'<div style="font-size:10px;color:var(--pf-text2);opacity:0.7;margin-top:1px;">{sub}</div>'
            f'</div>'
        )

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        _vc = 'var(--pf-green)' if _annual_vol < 0.15 else ('var(--pf-text)' if _annual_vol < 0.25 else 'var(--pf-red)')
        st.markdown(_risk_card('Ann. Volatility', f'{_annual_vol:.1%}',
                               f'{_freq_label} σ={_period_vol:.2%}', _vc), unsafe_allow_html=True)
    with c2:
        st.markdown(_risk_card('Max Drawdown', f'{_max_dd:.1%}',
                               f'¥{_dd_pnl_lost:,.0f} · {_dd_start_date}→{_dd_end_date}', 'var(--pf-red)'), unsafe_allow_html=True)
    with c3:
        _sc = 'var(--pf-green)' if _sharpe > 1 else ('var(--pf-text)' if _sharpe > 0 else 'var(--pf-red)')
        st.markdown(_risk_card('Sharpe Ratio', f'{_sharpe:.2f}', 'rf=1.5% (CNY)', _sc), unsafe_allow_html=True)
    with c4:
        _wc = 'var(--pf-green)' if _win_rate > 50 else 'var(--pf-red)'
        st.markdown(_risk_card('Win Rate', f'{_win_rate:.0f}%',
                               f'{_win}W / {_total-_win}L ({_freq_label})', _wc), unsafe_allow_html=True)
    with c5:
        _cc = 'var(--pf-green)' if _calmar > 1 else ('var(--pf-text)' if _calmar > 0.5 else 'var(--pf-red)')
        st.markdown(_risk_card('Calmar Ratio', f'{_calmar:.2f}', f'ret={_annual_ret:.1%}/yr', _cc), unsafe_allow_html=True)

    # ── Rolling trend chart (subplots: drawdown + rolling vol/Sharpe) ──
    from plotly.subplots import make_subplots
    _chart = snap.dropna(subset=['rolling_vol', 'rolling_sharpe']).copy()
    if len(_chart) >= 3:
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
            row_heights=[0.4, 0.6],
            specs=[[{}], [{'secondary_y': True}]],
        )

        # Row 1: Drawdown (filled area)
        fig.add_trace(go.Scatter(
            x=_chart['date'], y=_chart['drawdown_pct'],
            name='Drawdown', fill='tozeroy',
            line=dict(color='rgba(239,68,68,0.7)', width=1),
            fillcolor='rgba(239,68,68,0.15)',
            hovertemplate='%{x|%Y-%m-%d}<br>Drawdown: %{y:.1f}%<extra></extra>',
        ), row=1, col=1)

        # Row 2: Rolling Volatility (left Y)
        fig.add_trace(go.Scatter(
            x=_chart['date'], y=_chart['rolling_vol'],
            name=f'Rolling Vol ({_window}期)',
            line=dict(color='#f59e0b', width=1.5),
            hovertemplate='%{x|%Y-%m-%d}<br>Ann. Vol: %{y:.1f}%<extra></extra>',
        ), row=2, col=1, secondary_y=False)

        # Row 2: Rolling Sharpe (right Y)
        fig.add_trace(go.Scatter(
            x=_chart['date'], y=_chart['rolling_sharpe'],
            name=f'Rolling Sharpe ({_window}期)',
            line=dict(color='#3b82f6', width=1.5),
            hovertemplate='%{x|%Y-%m-%d}<br>Sharpe: %{y:.2f}<extra></extra>',
        ), row=2, col=1, secondary_y=True)

        # Reference lines
        fig.add_hline(y=0, line=dict(color='rgba(150,150,150,0.3)', width=1), row=1, col=1)
        fig.add_hline(y=1, line=dict(color='rgba(59,130,246,0.2)', width=1, dash='dot'),
                      row=2, col=1, secondary_y=True)  # Sharpe = 1 reference

        fig.update_yaxes(title_text='Drawdown %', row=1, col=1, ticksuffix='%',
                         zeroline=False)
        fig.update_yaxes(title_text='Ann. Vol %', row=2, col=1, secondary_y=False,
                         ticksuffix='%', zeroline=False)
        fig.update_yaxes(title_text='Sharpe', row=2, col=1, secondary_y=True,
                         zeroline=False)

        fig.update_layout(
            height=340,
            margin=dict(l=0, r=0, t=8, b=0),
            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(size=11),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5,
                        font=dict(size=10)),
            hovermode='x unified',
        )
        for ax in ['xaxis', 'xaxis2']:
            fig.update_layout(**{ax: dict(showgrid=False)})
        for ax in ['yaxis', 'yaxis2', 'yaxis3']:
            fig.update_layout(**{ax: dict(gridcolor='rgba(150,150,150,0.1)')})

        st.plotly_chart(fig, use_container_width=True)

    # ── Notes ──
    _date_range = f'{snap["date"].iloc[0]} ~ {snap["date"].iloc[-1]}'
    st.markdown(f'''<div style="font-size:11px; color:var(--pf-text2); font-family:var(--pf-mono);
        margin-top:-8px; margin-bottom:16px; line-height:1.7; opacity:0.75;">
        * {_total}条快照 ({_date_range})，{_freq_label} (中位间隔{_median_gap:.0f}天)，
          Rolling窗口={_window}期 ≈ {_window * _median_gap / 30:.0f}个月<br>
        * 收益率已<b>剔除出入金影响</b>: return = ΔPnL / (Capital_t + PnL_prev)<br>
        * <b>KPI卡片</b>: 全周期汇总值。<b>趋势图</b>: rolling {_window}期滚动计算，观察指标变化方向<br>
        * <b>Drawdown</b>: 累计收益指数相对峰值的跌幅，0%=历史新高。
          最大回撤 {_max_dd:.1%}，期间亏损 ¥{abs(_dd_pnl_lost):,.0f}<br>
        * <b>Sharpe</b> 虚线=1.0 (优秀线)。&gt;1 每承担1单位风险获得&gt;1单位超额收益
    </div>''', unsafe_allow_html=True)


# ────────────────────────────────────────
# Return Attribution
# ────────────────────────────────────────

def render_attribution(df, fx):
    """Return attribution: by market and by top stock contributors."""
    if df.empty:
        return

    # Only show if we have YTD or total P&L data
    has_ytd = 'ytd_pnl_cny' in df.columns and df['ytd_pnl_cny'].notna().any()
    has_daily = 'daily_pnl_cny' in df.columns and df['daily_pnl_cny'].notna().any()
    if not has_ytd and not has_daily:
        return

    st.markdown('<div class="section-title">Return Attribution</div>', unsafe_allow_html=True)

    _MKT_ORD = ['美股', '港股', 'A股', 'B股', '日股', '基金']

    # ── Tab selection: Daily / YTD / Unrealised / Total ──
    _attr_tabs = []
    if has_daily:
        _attr_tabs.append('Daily')
    if has_ytd:
        _attr_tabs.append('YTD')
    _attr_tabs.append('Unrealised')
    _attr_tabs.append('Total')

    tabs = st.tabs(_attr_tabs)

    # Load closed trades once for Total tab (apply live FX for 富途/B股)
    _closed_df = _apply_fx_to_closed(load_closed(), fx)

    # Pre-compute realised P&L aggregations (used by Total tab)
    _real_by_mkt = pd.DataFrame(columns=['market', '_real_pnl'])
    _real_by_stk = pd.DataFrame(columns=['name', '_real_pnl'])
    if not _closed_df.empty:
        _real = _closed_df[_closed_df['realized_pnl_cny'].notna()].copy()
        if not _real.empty:
            _real_by_mkt = _real.groupby('market')['realized_pnl_cny'].sum().reset_index()
            _real_by_mkt.columns = ['market', '_real_pnl']
            _real_by_stk = _real.groupby('name')['realized_pnl_cny'].sum().reset_index()
            _real_by_stk.columns = ['name', '_real_pnl']

    for tab_idx, tab_name in enumerate(_attr_tabs):
        with tabs[tab_idx]:
            if tab_name == 'YTD':
                pnl_col, pnl_short = 'ytd_pnl_cny', 'YTD P&L'
                _merge_realised = False
            elif tab_name == 'Daily':
                pnl_col, pnl_short = 'daily_pnl_cny', 'Daily P&L'
                _merge_realised = False
            elif tab_name == 'Unrealised':
                pnl_col, pnl_short = 'pnl_cny', 'Unrealised P&L'
                _merge_realised = False
            else:  # Total
                pnl_col, pnl_short = 'pnl_cny', 'Total P&L'
                _merge_realised = True

            _df = df[df[pnl_col].notna()].copy() if pnl_col in df.columns else df.copy()
            if _df.empty and not _merge_realised:
                st.caption("No data")
                continue

            col_mkt, col_stk = st.columns(2)

            # ── By Market ──
            with col_mkt:
                _grp = _df.groupby('market').agg(
                    _pnl=(pnl_col, 'sum'),
                    _mv=('market_value_cny', 'sum'),
                ).sort_values('_pnl', ascending=True) if not _df.empty else pd.DataFrame(columns=['_pnl', '_mv'])

                # Merge realised P&L for Total tab
                if _merge_realised and not _real_by_mkt.empty:
                    for _, rr in _real_by_mkt.iterrows():
                        mkt = rr['market']
                        if mkt in _grp.index:
                            _grp.loc[mkt, '_pnl'] += rr['_real_pnl']
                        else:
                            _grp.loc[mkt] = {'_pnl': rr['_real_pnl'], '_mv': 0}
                    _grp = _grp.sort_values('_pnl', ascending=True)

                sorted_mkts = sorted(_grp.index,
                                     key=lambda m: _MKT_ORD.index(m) if m in _MKT_ORD else 99)
                _grp = _grp.loc[sorted_mkts]

                bar_colors = ['#ef4444' if v >= 0 else '#22c55e' for v in _grp['_pnl']]

                # Compact text: use 万 for large values
                _mkt_texts = []
                for v in _grp['_pnl']:
                    av = abs(v)
                    sign = '+' if v >= 0 else '-'
                    if av >= 10000:
                        _mkt_texts.append(f"{sign}{av/10000:.1f}万")
                    else:
                        _mkt_texts.append(f"{sign}{av:,.0f}")

                fig_mkt = go.Figure(go.Bar(
                    y=_grp.index,
                    x=_grp['_pnl'],
                    orientation='h',
                    marker_color=bar_colors,
                    text=_mkt_texts,
                    textposition='auto',
                    textfont=dict(size=12, family='SF Mono, Consolas, monospace'),
                    insidetextanchor='end',
                    constraintext='both',
                    hovertemplate='%{y}<br>P&L: ¥%{x:,.0f}<extra></extra>',
                ))
                fig_mkt.update_layout(
                    title=dict(text=f'{pnl_short} by Market', font=dict(size=13, color='#8b949e'), x=0.5),
                    height=max(180, len(_grp) * 38 + 50),
                    margin=dict(t=35, b=10, l=50, r=20),
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)',
                               zeroline=True, zerolinecolor='rgba(128,128,128,0.3)',
                               tickformat=',',
                               tickfont=dict(size=11, family='SF Mono, Consolas, monospace')),
                    yaxis=dict(tickfont=dict(size=12, family='SF Mono, Consolas, monospace')),
                    showlegend=False,
                    uniformtext=dict(minsize=10, mode='hide'),
                )
                st.plotly_chart(fig_mkt, use_container_width=True)

            # ── By Stock (Top 10 contributors + Bottom 5 detractors) ──
            with col_stk:
                _stk = _df.groupby(['name', 'ticker']).agg(
                    _pnl=(pnl_col, 'sum'),
                ).reset_index() if not _df.empty else pd.DataFrame(columns=['name', 'ticker', '_pnl'])

                # Merge realised P&L for Total tab
                if _merge_realised and not _real_by_stk.empty:
                    for _, rr in _real_by_stk.iterrows():
                        nm = rr['name']
                        mask = _stk['name'] == nm
                        if mask.any():
                            _stk.loc[mask, '_pnl'] += rr['_real_pnl']
                        else:
                            _stk = pd.concat([_stk, pd.DataFrame([{
                                'name': nm, 'ticker': '', '_pnl': rr['_real_pnl']
                            }])], ignore_index=True)

                # Top 10 positive + bottom 5 negative
                _pos = _stk[_stk['_pnl'] > 0].nlargest(10, '_pnl')
                _neg = _stk[_stk['_pnl'] < 0].nsmallest(5, '_pnl')
                _top = pd.concat([_neg, _pos])  # bottom to top for horizontal bar

                if _top.empty:
                    st.caption("No stock contributions")
                    continue

                _labels = [f"{r['name']}" for _, r in _top.iterrows()]
                bar_colors = ['#ef4444' if v >= 0 else '#22c55e' for v in _top['_pnl']]

                # Compact text for stock bars
                _stk_texts = []
                for v in _top['_pnl']:
                    av = abs(v)
                    sign = '+' if v >= 0 else '-'
                    if av >= 10000:
                        _stk_texts.append(f"{sign}{av/10000:.1f}万")
                    else:
                        _stk_texts.append(f"{sign}{av:,.0f}")

                fig_stk = go.Figure(go.Bar(
                    y=_labels,
                    x=_top['_pnl'],
                    orientation='h',
                    marker_color=bar_colors,
                    text=_stk_texts,
                    textposition='auto',
                    textfont=dict(size=11, family='SF Mono, Consolas, monospace'),
                    insidetextanchor='end',
                    constraintext='both',
                    hovertemplate='%{y}<br>P&L: ¥%{x:,.0f}<extra></extra>',
                ))
                fig_stk.update_layout(
                    title=dict(text=f'{pnl_short} Top Contributors', font=dict(size=13, color='#8b949e'), x=0.5),
                    height=max(220, len(_top) * 28 + 50),
                    margin=dict(t=35, b=10, l=90, r=20),
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)',
                               zeroline=True, zerolinecolor='rgba(128,128,128,0.3)',
                               tickformat=',',
                               tickfont=dict(size=11, family='SF Mono, Consolas, monospace')),
                    yaxis=dict(tickfont=dict(size=11, family='SF Mono, Consolas, monospace')),
                    showlegend=False,
                    uniformtext=dict(minsize=9, mode='hide'),
                )
                st.plotly_chart(fig_stk, use_container_width=True)

            # ── Contribution percentage strip ──
            # Use merged _grp (which includes realised for Total tab) for contribution %
            _total_pnl = _grp['_pnl'].sum() if not _grp.empty else 0
            if _total_pnl != 0:
                sorted_mkts = sorted(_grp.index,
                                     key=lambda m: _MKT_ORD.index(m) if m in _MKT_ORD else 99)
                pills = []
                for m in sorted_mkts:
                    p = _grp.loc[m, '_pnl']
                    contrib = p / abs(_total_pnl) * 100
                    color = 'var(--pf-green)' if p >= 0 else 'var(--pf-red)'
                    sign = '+' if p >= 0 else ''
                    pills.append(
                        f'<span style="color:{color};">{m} {sign}{_fmt(p)}'
                        f'<span style="opacity:0.7;">({sign}{contrib:.1f}%)</span></span>'
                    )
                st.markdown(
                    f'<div style="font-family:var(--pf-mono);font-size:12px;'
                    f'color:var(--pf-text2);margin-top:-8px;margin-bottom:8px;">'
                    f'Contribution: {" · ".join(pills)}'
                    f'</div>',
                    unsafe_allow_html=True,
                )


# ────────────────────────────────────────
# Daily P&L Calendar & Journal
# ────────────────────────────────────────

def render_pnl_journal(df=None, fx=None):
    """Net P&L journal: calendar heatmap and journal table based on Net P&L (NAV − Capital).

    For today's entry: uses real-time position-based daily P&L (same as Holdings
    Daily P&L) instead of snapshot net_assets diff, which can include cash/leverage/
    FX changes and multi-day gaps.
    """
    snap_df = load_snapshots()
    nav_df = load_nav()  # has capital_invested for each snapshot date

    # Build capital lookup from nav_history + daily_snapshots.capital
    _cap_lookup = {}
    if not nav_df.empty:
        for _, r in nav_df.iterrows():
            if pd.notna(r.get('capital_invested')):
                _cap_lookup[r['date']] = r['capital_invested']
    # daily_snapshots.capital takes precedence (manually edited)
    if not snap_df.empty and 'capital' in snap_df.columns:
        for _, r in snap_df.iterrows():
            if pd.notna(r.get('capital')):
                _cap_lookup[r['date']] = r['capital']

    # Compute today's real-time daily P&L from positions (same formula as KPI card)
    _today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    _rt_daily_pnl = None
    _rt_daily_pnl_pct = None
    if df is not None and not df.empty and 'daily_pnl_cny' in df.columns:
        _dp_mask = df['daily_pnl_cny'].notna()
        if _dp_mask.any():
            _dp_total = df.loc[_dp_mask, 'daily_pnl_cny'].sum()
            _dp_base = (df.loc[_dp_mask, 'market_value_cny'] - df.loc[_dp_mask, 'daily_pnl_cny']).sum()
            _rt_daily_pnl = _dp_total
            _rt_daily_pnl_pct = (_dp_total / _dp_base * 100) if _dp_base != 0 else 0

    if snap_df.empty or len(snap_df) < 2:
        if _rt_daily_pnl is None:
            return
        snap = pd.DataFrame()
        pnl_lookup = {}
    else:
        # Prepare data — sort ascending for diff calculation
        snap = snap_df.sort_values('date').reset_index(drop=True)
        snap['date_dt'] = pd.to_datetime(snap['date'])
        # Join capital from lookup
        snap['capital'] = snap['date'].map(_cap_lookup)
        snap['net_pnl'] = snap['net_assets'] - snap['capital']
        # Compute delta from net_pnl changes
        snap['prev_net_pnl'] = snap['net_pnl'].shift(1)
        snap['prev_date'] = snap['date'].shift(1)
        snap['daily_pnl'] = snap['net_pnl'] - snap['prev_net_pnl']
        snap['prev_nav'] = snap['net_assets'].shift(1)
        snap['daily_pnl_pct'] = (snap['daily_pnl'] / snap['prev_nav'] * 100)
        # Gap days between consecutive snapshots
        snap['gap_days'] = (snap['date_dt'] - snap['date_dt'].shift(1)).dt.days
        # Trading date = snapshot date − 1 (6 AM snapshot captures yesterday's close)
        snap['trading_date_dt'] = snap['date_dt'] - pd.Timedelta(days=1)
        snap['trading_date'] = snap['trading_date_dt'].dt.strftime('%Y-%m-%d')
        snap = snap.dropna(subset=['daily_pnl'])
        # Drop rows where trading date falls on a weekend (no real trading)
        snap = snap[snap['trading_date_dt'].dt.dayofweek < 5]

        if snap.empty and _rt_daily_pnl is None:
            return

        # Build lookup: trading_date_str → {pnl, pnl_pct, nav, capital, net_pnl, gap_days}
        # Uses trading_date (= snapshot date − 1 day), already computed & weekend-filtered above.
        pnl_lookup = {}
        for _, r in snap.iterrows():
            pnl_lookup[r['trading_date']] = {
                'pnl': r['daily_pnl'],
                'pnl_pct': r['daily_pnl_pct'],
                'nav': r['net_assets'],
                'capital': r['capital'] if pd.notna(r.get('capital')) else None,
                'net_pnl': r['net_pnl'] if pd.notna(r.get('net_pnl')) else None,
                'gap': int(r['gap_days']) if pd.notna(r['gap_days']) else 1,
            }

    # Override today's entry with real-time position-based daily P&L
    # Weekdays: today's P&L reflects live trading → label as today
    # Saturday: real-time P&L = Friday's close vs Thursday's close = Friday's P&L;
    #           historical shift already covers Friday → skip to avoid duplicate
    # Sunday: no trading → skip
    _dow = pd.Timestamp.now().dayofweek
    if _rt_daily_pnl is not None and _dow < 5:  # Mon–Fri only
        _today_nav = pnl_lookup.get(_today_str, {}).get('nav')
        _today_cap = _cap_lookup.get(_today_str)
        if _today_nav is None and not snap_df.empty:
            _row = snap_df[snap_df['date'] == _today_str]
            _today_nav = _row.iloc[0]['net_assets'] if not _row.empty else None
        _today_net_pnl = (_today_nav - _today_cap) if _today_nav is not None and _today_cap is not None else None
        pnl_lookup[_today_str] = {
            'pnl': _rt_daily_pnl,
            'pnl_pct': _rt_daily_pnl_pct,
            'nav': _today_nav,
            'capital': _today_cap,
            'net_pnl': _today_net_pnl,
            'gap': 1,
        }

    if not pnl_lookup:
        return

    st.markdown('<div class="section-title">Net P&L</div>', unsafe_allow_html=True)

    # ── Month navigation ──
    now = pd.Timestamp.now()
    if 'cal_year' not in st.session_state:
        st.session_state.cal_year = now.year
        st.session_state.cal_month = now.month

    col_prev, col_title, col_next = st.columns([1, 3, 1])
    with col_prev:
        if st.button("◀", key='cal_prev', use_container_width=True):
            m = st.session_state.cal_month - 1
            if m < 1:
                st.session_state.cal_year -= 1
                st.session_state.cal_month = 12
            else:
                st.session_state.cal_month = m
            st.rerun()
    with col_title:
        cal_year = st.session_state.cal_year
        cal_month = st.session_state.cal_month
        st.markdown(
            f'<div style="text-align:center;font-size:16px;font-weight:600;'
            f'font-family:var(--pf-mono);padding:6px 0;">'
            f'{calendar.month_name[cal_month]} {cal_year}</div>',
            unsafe_allow_html=True,
        )
    with col_next:
        if st.button("▶", key='cal_next', use_container_width=True):
            m = st.session_state.cal_month + 1
            if m > 12:
                st.session_state.cal_year += 1
                st.session_state.cal_month = 1
            else:
                st.session_state.cal_month = m
            st.rerun()

    # ── Calendar grid ──
    cal_obj = calendar.Calendar(firstweekday=0)  # Monday first
    month_pnl = 0.0
    month_win = 0
    month_loss = 0

    html = '<table class="pnl-cal"><thead><tr>'
    for dn in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
        html += f'<th>{dn}</th>'
    html += '</tr></thead><tbody>'

    for week in cal_obj.monthdayscalendar(cal_year, cal_month):
        html += '<tr>'
        for day in week:
            if day == 0:
                html += '<td></td>'
            else:
                date_str = f'{cal_year}-{cal_month:02d}-{day:02d}'
                data = pnl_lookup.get(date_str)
                if data:
                    pnl = data['pnl']
                    pnl_pct = data['pnl_pct']
                    gap = data.get('gap', 1)
                    month_pnl += pnl
                    if pnl >= 0:
                        month_win += 1
                    else:
                        month_loss += 1

                    # Color intensity based on pct magnitude (3% = full)
                    intensity = min(abs(pnl_pct) / 3.0, 1.0)
                    alpha = 0.10 + intensity * 0.40
                    if pnl >= 0:
                        bg = f'rgba(207,34,46,{alpha:.2f})'
                        color = 'var(--pf-green)'
                    else:
                        bg = f'rgba(26,127,55,{alpha:.2f})'
                        color = 'var(--pf-red)'

                    sign = '+' if pnl >= 0 else ''
                    gap_note = f' ({gap}d)' if gap > 2 else ''
                    tooltip = f'{sign}{pnl:,.0f} ({sign}{pnl_pct:.1f}%){gap_note}'
                    # Show gap indicator for multi-day spans
                    val_suffix = f'<span style="font-size:8px;opacity:0.6;">({gap}d)</span>' if gap > 2 else ''
                    html += (
                        f'<td class="has-data" style="background:{bg};color:{color};" title="{tooltip}">'
                        f'<div class="day-num">{day}</div>'
                        f'<div class="day-val">{sign}{pnl:,.0f}{val_suffix}</div>'
                        f'</td>'
                    )
                else:
                    html += f'<td class="empty">{day}</td>'
        html += '</tr>'
    html += '</tbody></table>'

    # Monthly summary
    total_days = month_win + month_loss
    if total_days > 0:
        m_sign = '+' if month_pnl >= 0 else ''
        m_color = 'var(--pf-green)' if month_pnl >= 0 else 'var(--pf-red)'
        html += (
            f'<div style="text-align:center;margin-top:10px;font-family:var(--pf-mono);font-size:13px;'
            f'color:var(--pf-text2);">'
            f'Month: <span style="color:{m_color};font-weight:600;">{m_sign}{month_pnl:,.0f}</span>'
            f' · <span style="color:var(--pf-green);">{month_win}W</span>'
            f' / <span style="color:var(--pf-red);">{month_loss}L</span>'
            f' · Win {month_win/(total_days)*100:.0f}%'
            f'</div>'
        )
    st.markdown(html, unsafe_allow_html=True)

    # ── Journal table (expandable) — last 30 days only ──
    if not snap.empty:
        _30d_ago = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        journal = snap[snap['trading_date'] >= _30d_ago].sort_values('trading_date', ascending=False).copy()
        if journal.empty:
            return
        with st.expander(f"Net P&L Journal (last 30 days · {len(journal)} entries)", expanded=False):

            jhtml = '<table class="cash-table"><thead><tr>'
            jhtml += '<th style="text-align:left;">Date</th><th>Net Assets</th>'
            jhtml += '<th>Capital</th><th>Net P&L</th>'
            jhtml += '<th>ΔNet P&L</th><th>ΔNet P&L%</th>'
            jhtml += '</tr></thead><tbody>'

            for _, r in journal.iterrows():
                _d = r['trading_date']
                pnl = r['daily_pnl']
                pct = r['daily_pnl_pct']
                cls = _pnl_class(pnl)
                sign = '+' if pnl >= 0 else ''
                _cap = r.get('capital')
                _net = r.get('net_pnl')
                _net_cls = _pnl_class(_net) if pd.notna(_net) else ''
                jhtml += '<tr>'
                jhtml += f'<td style="text-align:left;">{_d}</td>'
                jhtml += f'<td>¥{r["net_assets"]:,.0f}</td>'
                jhtml += f'<td>{"¥" + _fmt(_cap) if pd.notna(_cap) else "—"}</td>'
                jhtml += f'<td class="{_net_cls}">{"¥" + _pnl_sign(_net) if pd.notna(_net) else "—"}</td>'
                jhtml += f'<td class="{cls}">{sign}{pnl:,.0f}</td>'
                jhtml += f'<td class="{cls}">{sign}{pct:.2f}%</td>'
                jhtml += '</tr>'

            jhtml += '</tbody></table>'
            st.markdown(jhtml, unsafe_allow_html=True)


# ────────────────────────────────────────
# Closed Trades
# ────────────────────────────────────────

def render_closed(fx=None):
    closed_df = load_closed()
    if closed_df.empty:
        return
    if fx:
        closed_df = _apply_fx_to_closed(closed_df, fx)

    with st.expander("Closed Trades (historical P&L)", expanded=False):
        # ── Summary by (market, broker) ──
        summary = {}
        for _, r in closed_df.iterrows():
            m = r['market']
            b = r['broker']
            pnl_cny = r['realized_pnl_cny']
            key = (m, b)
            if key not in summary:
                summary[key] = {'count': 0, 'pnl_cny': 0}
            summary[key]['count'] += 1
            summary[key]['pnl_cny'] += pnl_cny

        html = '<table class="cash-table"><thead><tr><th>Market</th><th>Broker</th><th>Trades</th><th>P&L (¥)</th></tr></thead><tbody>'
        total_pnl = 0
        for (m, b), s in sorted(summary.items()):
            cls = _pnl_class(s['pnl_cny'])
            html += f'<tr><td>{m}</td><td>{b}</td><td>{s["count"]}</td><td class="{cls}">{_pnl_sign(s["pnl_cny"])}</td></tr>'
            total_pnl += s['pnl_cny']
        cls = _pnl_class(total_pnl)
        html += f'<tr style="font-weight:600; border-top:2px solid var(--pf-border);"><td>Total</td><td></td><td>{len(closed_df)}</td><td class="{cls}">{_pnl_sign(total_pnl)}</td></tr>'
        html += '</tbody></table>'
        st.markdown(html, unsafe_allow_html=True)

        # ── Per-market detail sub-expanders (grouped by broker within) ──
        markets = sorted(set(k[0] for k in summary.keys()))
        for market in markets:
            market_df = closed_df[closed_df['market'] == market]
            mkt_pnl = sum(s['pnl_cny'] for (m, _), s in summary.items() if m == market)
            with st.expander(f"{market} ({len(market_df)} trades · "
                             f"{'+'if mkt_pnl>=0 else ''}{_fmt(mkt_pnl)})", expanded=False):
                # Group by broker within this market
                brokers = sorted(market_df['broker'].unique())
                for broker in brokers:
                    broker_df = market_df[market_df['broker'] == broker]
                    b_pnl = sum(r['realized_pnl_cny'] for _, r in broker_df.iterrows())
                    b_cls = _pnl_class(b_pnl)
                    if len(brokers) > 1:
                        st.markdown(
                            f'<div style="font-size:13px;font-weight:600;margin:8px 0 4px;color:var(--pf-text);">'
                            f'{broker} <span class="{b_cls}" style="font-weight:400;">({len(broker_df)} · '
                            f'{"+"if b_pnl>=0 else ""}{_fmt(b_pnl)})</span></div>',
                            unsafe_allow_html=True)

                    has_qty = 'quantity' in broker_df.columns and broker_df['quantity'].notna().any()
                    dhtml = '<table class="cash-table"><thead><tr>'
                    dhtml += '<th style="text-align:left;">Name</th><th>Broker</th>'
                    if has_qty:
                        dhtml += '<th>Qty</th><th>Cost</th><th>Close</th>'
                    dhtml += '<th>Original Ccy</th><th>P&L(原币)</th><th>P&L in CNY</th><th>Date</th>'
                    dhtml += '</tr></thead><tbody>'

                    for _, r in broker_df.iterrows():
                        pnl = r['realized_pnl']
                        pnl_cny = r['realized_pnl_cny']
                        cls = _pnl_class(pnl_cny)
                        ccy = r['currency']
                        dhtml += '<tr>'
                        dhtml += f'<td style="text-align:left;">{r["name"]}</td>'
                        dhtml += f'<td>{r["broker"]}</td>'
                        if has_qty:
                            qty_s = _fmt(r['quantity']) if pd.notna(r.get('quantity')) else '--'
                            cost_s = _fmt(r['cost_price'], 2) if pd.notna(r.get('cost_price')) else '--'
                            close_s = _fmt(r['close_price'], 2) if pd.notna(r.get('close_price')) else '--'
                            dhtml += f'<td>{qty_s}</td><td>{cost_s}</td><td>{close_s}</td>'
                        date_s = r.get('close_date') or '--'
                        if ccy == 'CNY':
                            pnl_orig_s = '—'
                            ccy_orig = ''
                        else:
                            pnl_orig_s = f"{'+'if pnl>=0 else ''}{_fmt(pnl)}"
                            ccy_orig = ccy
                        pnl_cny_s = _pnl_sign(pnl_cny)
                        dhtml += f'<td>{ccy_orig}</td>'
                        dhtml += f'<td class="{cls}">{pnl_orig_s}</td>'
                        dhtml += f'<td class="{cls}">{pnl_cny_s}</td>'
                        dhtml += f'<td>{date_s}</td>'
                        dhtml += '</tr>'

                    dhtml += '</tbody></table>'
                    st.markdown(dhtml, unsafe_allow_html=True)


# ────────────────────────────────────────
# Sidebar — Manual Position Management
# ────────────────────────────────────────

@st.fragment
def render_sidebar():
    st.markdown("### Position Management")

    tab1, tab2, tab3, tab4 = st.tabs(["Edit", "Delete", "Cash/Margin", "Import"])

    with tab1:
        positions_df = load_positions()

        mode = st.radio("Mode", ["Edit Existing", "Add New"], horizontal=True,
                        label_visibility="collapsed", key='_sidebar_mode')

        if mode == "Edit Existing" and not positions_df.empty:
            options = [f"{r['name']} ({r['ticker']}) — {r['broker']}" for _, r in positions_df.iterrows()]

            selected_idx = st.selectbox("Select position", range(len(options)),
                                        format_func=lambda i: options[i],
                                        key='edit_position_select')
            sel_row = positions_df.iloc[selected_idx]
            # Track original position ID for reliable update (avoids creating duplicates)
            _orig_id = int(sel_row['id'])

            with st.form("edit_position", clear_on_submit=False):
                name = st.text_input("Name", value=sel_row['name'])
                ticker = st.text_input("Ticker", value=sel_row['ticker'])
                market = st.selectbox("Market", ['A股', 'B股', '港股', '美股', '日股', '基金'],
                                      index=['A股', 'B股', '港股', '美股', '日股', '基金'].index(sel_row['market']))
                broker = st.text_input("Broker", value=sel_row['broker'])
                currency = st.selectbox("Currency", ['CNY', 'USD', 'HKD', 'JPY'],
                                        index=['CNY', 'USD', 'HKD', 'JPY'].index(sel_row['currency']))
                quantity = st.number_input("Quantity", value=int(sel_row['quantity']),
                                          step=1, min_value=0)
                cost_price = st.number_input("Cost Price", value=float(sel_row['cost_price']),
                                             step=0.01, format="%.4f", min_value=0.0)
                submitted = st.form_submit_button("Update")

                if submitted and name and ticker:
                    with get_conn() as conn:
                        # Only touch updated_at when qty or cost actually changes
                        # (name/broker edits should not affect daily P&L baseline)
                        conn.execute("""
                            UPDATE positions SET
                                ticker=?, name=?, market=?, broker=?, currency=?,
                                quantity=?, cost_price=?,
                                updated_at = CASE
                                    WHEN quantity != ? OR abs(cost_price - ?) > 0.0001
                                    THEN datetime('now','localtime')
                                    ELSE updated_at
                                END
                            WHERE id=?
                        """, (ticker, name, market, broker, currency,
                              quantity or 0, cost_price or 0.0,
                              quantity or 0, cost_price or 0.0, _orig_id))
                        conn.commit()
                    st.success(f"Updated: {name} ({ticker})")
                    load_positions.clear()
                    build_portfolio.clear()
                    st.rerun()
        else:
            with st.form("add_position", clear_on_submit=True):
                name = st.text_input("Name")
                ticker = st.text_input("Ticker")
                market = st.selectbox("Market", ['A股', 'B股', '港股', '美股', '日股', '基金'])
                broker = st.text_input("Broker", value='中信证券')
                currency = st.selectbox("Currency", ['CNY', 'USD', 'HKD', 'JPY'])
                quantity = st.number_input("Quantity", value=None, step=1,
                                          min_value=0, placeholder="0")
                cost_price = st.number_input("Cost Price", value=None, step=0.01,
                                             format="%.4f", min_value=0.0, placeholder="0")
                submitted = st.form_submit_button("Add")

                if submitted and name and ticker:
                    qty = quantity or 0
                    cp = cost_price or 0.0
                    if qty > 0:
                        with get_conn() as conn:
                            upsert_position(conn, ticker, name, market, broker, currency, qty, cp)
                            conn.commit()
                        st.success(f"Added: {name} ({ticker})")
                        load_positions.clear()
                        build_portfolio.clear()
                        st.rerun()

    with tab2:
        positions_df = load_positions()
        if not positions_df.empty:
            options = [f"{r['name']} ({r['ticker']}) — {r['broker']}" for _, r in positions_df.iterrows()]
            selected_idx = st.selectbox("Select position to close", range(len(options)),
                                        format_func=lambda i: options[i], key='del_select')
            sel_row = positions_df.iloc[selected_idx]
            st.markdown(f"**{sel_row['name']}** ({sel_row['ticker']})  \n"
                        f"Qty: {sel_row['quantity']:,.0f} · Cost: {sel_row['cost_price']:.4f} · "
                        f"{sel_row['broker']}")

            # Try to get current market price as default
            _default_price = sel_row['cost_price']
            try:
                _mkt_price, _ = fetch_price(sel_row['ticker'])
                if _mkt_price and _mkt_price > 0:
                    _default_price = round(_mkt_price, 4)
            except Exception:
                pass

            close_price = st.number_input("Close Price", value=_default_price,
                                          step=0.01, format="%.4f", key='close_price_input')

            # Preview P&L
            qty = sel_row['quantity']
            cost_total = qty * sel_row['cost_price']
            proceeds = qty * close_price
            realized_pnl_local = proceeds - cost_total
            _close_fx = get_fx_rates()
            _close_fx_rate = _close_fx.get(sel_row['currency'], 1.0)
            realized_pnl_cny = realized_pnl_local * _close_fx_rate
            pnl_pct = (realized_pnl_local / cost_total * 100) if cost_total != 0 else 0
            pnl_cls = 'kpi-green' if realized_pnl_cny >= 0 else 'kpi-red'
            _ccy = sel_row['currency']
            _preview = (f"{'+' if realized_pnl_local >= 0 else ''}{realized_pnl_local:,.0f} {_ccy}"
                        if _ccy != 'CNY' else '')
            st.markdown(f"Realized P&L: <span class='{pnl_cls}'>"
                        f"{'(' + _preview + ') ' if _preview else ''}"
                        f"{'+' if realized_pnl_cny >= 0 else ''}{realized_pnl_cny:,.0f} CNY "
                        f"({'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%)</span>",
                        unsafe_allow_html=True)

            if st.button("Close Position", type="primary"):
                close_date = pd.Timestamp.now().strftime('%Y-%m-%d')
                with get_conn() as conn:
                    insert_closed_trade(
                        conn,
                        ticker=sel_row['ticker'],
                        name=sel_row['name'],
                        market=sel_row['market'],
                        broker=sel_row['broker'],
                        currency=sel_row['currency'],
                        realized_pnl=realized_pnl_local,
                        close_date=close_date,
                        quantity=qty,
                        cost_price=sel_row['cost_price'],
                        close_price=close_price,
                        cost_total=cost_total,
                        realized_pnl_cny=realized_pnl_cny,
                    )
                    conn.execute("DELETE FROM positions WHERE id=?", (int(sel_row['id']),))
                    conn.commit()
                st.success(f"Closed: {sel_row['name']} (P&L: {realized_pnl_cny:,.0f} CNY)")
                load_positions.clear()
                build_portfolio.clear()
                load_closed.clear()
                st.rerun()

    with tab3:
        st.markdown("**Cash Balances**")
        cash_df = load_cash()

        # Build the set of all accounts and currencies
        known_accounts = ['中信证券', '招商证券', '富途', '招商永隆', '支付宝']
        known_currencies = ['CNY', 'USD', 'HKD', 'JPY']

        # Add any accounts from DB not in the list
        if not cash_df.empty:
            for acct in cash_df['account'].unique():
                if acct not in known_accounts:
                    known_accounts.append(acct)

        # Build current balance lookup
        balance_lookup = {}
        if not cash_df.empty:
            for _, row in cash_df.iterrows():
                balance_lookup[(row['account'], row['currency'])] = row['balance']

        # Batch form: show all accounts × their currencies at once
        with st.form("batch_cash", clear_on_submit=False):
            updated_values = {}
            for acct in known_accounts:
                # Find which currencies this account has (non-zero or in DB)
                acct_curs = []
                for cur in known_currencies:
                    bal = balance_lookup.get((acct, cur), 0)
                    if bal != 0 or (not cash_df.empty and
                                    ((cash_df['account'] == acct) & (cash_df['currency'] == cur)).any()):
                        acct_curs.append((cur, bal))

                if not acct_curs:
                    continue  # Skip accounts with no balances

                st.markdown(f"**{acct}**")
                for cur, bal in acct_curs:
                    c_left, c_right = st.columns([1, 3])
                    c_left.markdown(f'<div style="padding-top:8px;font-family:var(--pf-mono);font-size:13px;">{cur}</div>', unsafe_allow_html=True)
                    val = c_right.number_input(
                        f"{acct} — {cur}",
                        value=float(bal),
                        step=1000.0,
                        format="%.0f",
                        key=f"cash_{acct}_{cur}",
                        label_visibility="collapsed",
                    )
                    updated_values[(acct, cur)] = val

            cash_submit = st.form_submit_button("Save All", type="primary")
            if cash_submit:
                with get_conn() as conn:
                    changed = 0
                    for (acct, cur), new_val in updated_values.items():
                        old_val = balance_lookup.get((acct, cur), 0)
                        if abs(new_val - old_val) > 0.001:
                            upsert_cash(conn, acct, cur, new_val)
                            changed += 1
                    conn.commit()
                if changed:
                    st.success(f"Updated {changed} balance(s)")
                    load_cash.clear()
                    st.rerun()
                else:
                    st.info("No changes detected")

        # Add new account/currency pair
        with st.expander("Add new account"):
            with st.form("add_cash_account", clear_on_submit=True):
                new_acct = st.text_input("Account name")
                new_cur = st.selectbox("Currency", known_currencies, key='new_cash_cur')
                new_bal = st.number_input("Balance", value=0.0, step=1000.0, format="%.0f", key='new_cash_bal')
                add_submit = st.form_submit_button("Add")
                if add_submit and new_acct:
                    with get_conn() as conn:
                        upsert_cash(conn, new_acct, new_cur, new_bal)
                        conn.commit()
                    st.success(f"Added: {new_acct} {new_cur}")
                    load_cash.clear()
                    st.rerun()

        st.markdown("---")
        st.markdown("**Leverage / Margin**")
        margin_df = load_margin()

        # Build margin lookup: {(category, currency): amount}
        margin_lookup = {}
        if not margin_df.empty:
            for _, row in margin_df.iterrows():
                margin_lookup[(row['category'], row['currency'])] = row['amount']

        margin_currencies = ['USD', 'HKD', 'JPY', 'CNY']

        with st.form("update_margin", clear_on_submit=False):
            margin_values = {}

            # 场内杠杆 — 4 currencies
            st.markdown("**场内杠杆**")
            for cur in margin_currencies:
                bal = margin_lookup.get(('in_house', cur), 0.0)
                c_left, c_right = st.columns([1, 3])
                c_left.markdown(f'<div style="padding-top:8px;font-family:var(--pf-mono);font-size:13px;">{cur}</div>', unsafe_allow_html=True)
                val = c_right.number_input(
                    f"场内 {cur}",
                    value=float(bal),
                    step=1000.0,
                    format="%.0f",
                    key=f"margin_in_{cur}",
                    label_visibility="collapsed",
                )
                margin_values[('in_house', cur)] = val

            # 场外杠杆 — CNY only
            st.markdown("**场外杠杆**")
            off_bal = margin_lookup.get(('off_exchange', 'CNY'), 0.0)
            c_left, c_right = st.columns([1, 3])
            c_left.markdown('<div style="padding-top:8px;font-family:var(--pf-mono);font-size:13px;">CNY</div>', unsafe_allow_html=True)
            off_val = c_right.number_input(
                "场外 CNY",
                value=float(off_bal),
                step=10000.0,
                format="%.0f",
                key="margin_off_cny",
                label_visibility="collapsed",
            )
            margin_values[('off_exchange', 'CNY')] = off_val

            margin_submit = st.form_submit_button("Update Margin")
            if margin_submit:
                with get_conn() as conn:
                    changed = 0
                    for (cat, cur), new_val in margin_values.items():
                        old_val = margin_lookup.get((cat, cur), 0.0)
                        if abs(new_val - old_val) > 0.001:
                            upsert_margin(conn, '合计', cat, cur, new_val)
                            changed += 1
                    conn.commit()
                if changed:
                    st.success(f"Updated {changed} margin(s)")
                    load_margin.clear()
                    st.rerun()
                else:
                    st.info("No changes detected")

    # ── Import tab ──
    with tab4:
        st.markdown("**Bulk Import via CSV**")
        st.caption("Upload CSV files to bulk-import positions, cash, or closed trades. "
                   "Existing records with the same key will be updated.")

        # ── Template downloads ──
        with st.expander("📥 Download CSV Templates"):
            _pos_tpl = ("ticker,name,market,broker,currency,quantity,cost_price\n"
                        "AAPL,Apple,美股,MyBroker,USD,100,150.00\n"
                        "0700.HK,Tencent,港股,MyBroker,HKD,200,350.00\n")
            st.download_button("Positions Template", _pos_tpl,
                               "positions_template.csv", "text/csv",
                               key="dl_pos_tpl")

            _cash_tpl = ("account,currency,balance\n"
                         "MyBroker,USD,10000\n"
                         "MyBroker,CNY,50000\n")
            st.download_button("Cash Template", _cash_tpl,
                               "cash_template.csv", "text/csv",
                               key="dl_cash_tpl")

            _closed_tpl = ("ticker,name,market,broker,currency,realized_pnl,close_date\n"
                           "MSFT,Microsoft,美股,MyBroker,USD,500.00,2026-01-15\n")
            st.download_button("Closed Trades Template", _closed_tpl,
                               "closed_trades_template.csv", "text/csv",
                               key="dl_closed_tpl")

        # ── Positions import ──
        st.markdown("**Import Positions**")
        pos_file = st.file_uploader("Upload positions CSV", type=['csv'],
                                    key='import_pos_file')
        if pos_file is not None:
            try:
                pos_imp = pd.read_csv(pos_file)
                required = {'ticker', 'name', 'market', 'broker', 'currency',
                            'quantity', 'cost_price'}
                missing = required - set(pos_imp.columns)
                if missing:
                    st.error(f"Missing columns: {', '.join(sorted(missing))}")
                else:
                    st.dataframe(pos_imp, use_container_width=True, hide_index=True)
                    st.caption(f"{len(pos_imp)} rows ready. Review above, then click Import.")
                    if st.button("Import Positions", type="primary",
                                 key="btn_import_pos"):
                        with get_conn() as conn:
                            for _, r in pos_imp.iterrows():
                                upsert_position(conn, str(r['ticker']).strip(),
                                                str(r['name']).strip(),
                                                str(r['market']).strip(),
                                                str(r['broker']).strip(),
                                                str(r['currency']).strip().upper(),
                                                float(r['quantity']),
                                                float(r['cost_price']))
                            conn.commit()
                        st.success(f"Imported {len(pos_imp)} positions ✓")
                        load_positions.clear()
                        build_portfolio.clear()
                        st.rerun()
            except Exception as e:
                st.error(f"CSV parse error: {e}")

        # ── Cash import ──
        st.markdown("**Import Cash Balances**")
        cash_file = st.file_uploader("Upload cash CSV", type=['csv'],
                                     key='import_cash_file')
        if cash_file is not None:
            try:
                cash_imp = pd.read_csv(cash_file)
                required = {'account', 'currency', 'balance'}
                missing = required - set(cash_imp.columns)
                if missing:
                    st.error(f"Missing columns: {', '.join(sorted(missing))}")
                else:
                    st.dataframe(cash_imp, use_container_width=True, hide_index=True)
                    if st.button("Import Cash", type="primary",
                                 key="btn_import_cash"):
                        with get_conn() as conn:
                            for _, r in cash_imp.iterrows():
                                upsert_cash(conn, str(r['account']).strip(),
                                            str(r['currency']).strip().upper(),
                                            float(r['balance']))
                            conn.commit()
                        st.success(f"Imported {len(cash_imp)} cash balances ✓")
                        load_cash.clear()
                        st.rerun()
            except Exception as e:
                st.error(f"CSV parse error: {e}")

        # ── Closed trades import ──
        st.markdown("**Import Closed Trades**")
        closed_file = st.file_uploader("Upload closed trades CSV", type=['csv'],
                                       key='import_closed_file')
        if closed_file is not None:
            try:
                closed_imp = pd.read_csv(closed_file)
                required = {'ticker', 'name', 'market', 'broker', 'currency',
                            'realized_pnl'}
                missing = required - set(closed_imp.columns)
                if missing:
                    st.error(f"Missing columns: {', '.join(sorted(missing))}")
                else:
                    st.dataframe(closed_imp, use_container_width=True, hide_index=True)
                    if st.button("Import Closed Trades", type="primary",
                                 key="btn_import_closed"):
                        fx = get_fx_rates()
                        with get_conn() as conn:
                            for _, r in closed_imp.iterrows():
                                cur = str(r['currency']).strip().upper()
                                rpl = float(r['realized_pnl'])
                                rpl_cny = rpl * fx.get(cur, 1.0)
                                insert_closed_trade(
                                    conn,
                                    ticker=str(r['ticker']).strip(),
                                    name=str(r['name']).strip(),
                                    market=str(r['market']).strip(),
                                    broker=str(r['broker']).strip(),
                                    currency=cur,
                                    realized_pnl=rpl,
                                    realized_pnl_cny=rpl_cny,
                                    close_date=(str(r['close_date'])
                                                if 'close_date' in r and pd.notna(r.get('close_date'))
                                                else None),
                                )
                            conn.commit()
                        st.success(f"Imported {len(closed_imp)} closed trades ✓")
                        load_closed.clear()
                        st.rerun()
            except Exception as e:
                st.error(f"CSV parse error: {e}")


# ────────────────────────────────────────
# Main
# ────────────────────────────────────────

def main():
    # Build portfolio data first (cached)
    fx = get_fx_rates()
    with st.spinner("正在加载行情数据..."):
        df = build_portfolio(fx_tuple=tuple(sorted(fx.items())))
    cash_df = load_cash()

    # Click-to-edit: holdings row link sets ?edit_pos=<idx>
    _qp = st.query_params
    _edit_pos = _qp.get('edit_pos')
    _want_open_sidebar = False
    if _edit_pos is not None:
        try:
            _edit_idx = int(_edit_pos)
            positions_df = load_positions()
            if 0 <= _edit_idx < len(positions_df):
                st.session_state['_sidebar_mode'] = "Edit Existing"
                st.session_state['edit_position_select'] = _edit_idx
                _want_open_sidebar = True
        except (ValueError, TypeError):
            pass
        # Clear param to avoid re-triggering on next rerun
        st.query_params.clear()

    # Sidebar for position management
    with st.sidebar:
        render_sidebar()

    # Auto-open sidebar if it was collapsed when user clicked a stock link
    if _want_open_sidebar:
        st.html('''<script>
(function(){
    // Only expand if sidebar is currently collapsed; never collapse an open sidebar
    var sidebar = document.querySelector('[data-testid="stSidebar"]');
    if (sidebar && sidebar.offsetParent !== null) return; // already visible
    var btn = document.querySelector('[data-testid="stSidebarCollapsedControl"] button');
    if (btn) btn.click();
})();
</script>''', unsafe_allow_javascript=True)

    # Title
    st.markdown(
        '<div style="display:flex;align-items:baseline;gap:16px;">'
        '<h2 style="margin:0;">Portfolio Tracker</h2>'
        '<span style="font-size:11px;color:var(--pf-text2);font-style:italic;">in CNY, unless otherwise noted</span>'
        f'<span style="font-size:12px;color:var(--pf-text2);font-family:var(--pf-mono);">'
        f'{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    # FX rates
    render_fx_banner(fx)

    # Compute capital once — shared by KPI, performance, and P&L breakdown
    with get_conn() as _cap_conn:
        current_capital = compute_capital(_cap_conn, fx)

    render_kpi(df, cash_df, fx, current_capital=current_capital)
    render_allocation(df)
    render_holdings(df, fx)
    render_unrealized_market_strip(df, fx)
    render_cash(cash_df)
    render_performance(current_capital)

    # Capital calculation notes
    _cap_ts = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    if DEPOSIT_MODE:
        st.markdown(f'''<div style="font-size:11px; color:var(--pf-text2); font-family:var(--pf-mono);
            margin-top:-8px; margin-bottom:16px; line-height:1.6;">
            Capital = 富途入金(¥{_fmt(FUTU_CAPITAL)}) + B股入金(¥{_fmt(B_SHARE_CAPITAL)})
            + 非富途非B股持仓成本 + 现金 − 场外杠杆 − 已平仓盈亏 = <b>¥{_fmt(current_capital)}</b>
            <span style="opacity:0.5;">({_cap_ts})</span><br>
            <span style="opacity:0.7;">* 富途入金 hardcode: 该账户入金只进不出，使用历史人民币入金总额，避免汇率折算<br>
            * B股入金 hardcode: 同理，使用历史入金总额<br>
            * 已平仓盈亏: A股 + 基金 + 港股(招商) 已平仓损益，按卖出时汇率折算为¥，不再二次折算</span>
        </div>''', unsafe_allow_html=True)
    else:
        st.markdown(f'''<div style="font-size:11px; color:var(--pf-text2); font-family:var(--pf-mono);
            margin-top:-8px; margin-bottom:16px; line-height:1.6;">
            Capital = 持仓成本 + 现金 − 场外杠杆 − 已平仓盈亏 = <b>¥{_fmt(current_capital)}</b>
            <span style="opacity:0.5;">({_cap_ts})</span>
        </div>''', unsafe_allow_html=True)

    # ── P&L breakdown ──
    _equity_mv = df['market_value_cny'].sum() if not df.empty else 0
    _cash_total = sum(r['balance'] * fx.get(r['currency'], 1.0) for _, r in cash_df.iterrows()) if not cash_df.empty else 0
    margin_df = load_margin()
    _total_lev = sum(r['amount'] * fx.get(r['currency'], 1.0) for _, r in margin_df.iterrows()) if not margin_df.empty else 0
    _net_assets = _equity_mv + _cash_total - _total_lev

    _unrealized_pnl = df['pnl_cny'].sum() if not df.empty else 0
    _closed_df = _apply_fx_to_closed(load_closed(), fx)
    _realized_pnl = _closed_df['realized_pnl_cny'].sum() if not _closed_df.empty else 0

    _total_pl = _net_assets - current_capital
    _forex_gl = _total_pl - _unrealized_pnl - _realized_pnl
    _tp_cls = _pnl_class(_total_pl)
    _fx_cls = _pnl_class(_forex_gl)

    if DEPOSIT_MODE:
        # ── 富途 account-level breakdown (deposit mode only) ──
        _futu_fx_impact = FUTU_CAPITAL / FUTU_DEPOSIT_FX * fx.get('USD', 1.0) - FUTU_CAPITAL
        _futu_fx_cls = _pnl_class(_futu_fx_impact)
        _futu_mv = df.loc[df['broker'] == '富途', 'market_value_cny'].sum() if not df.empty else 0
        _futu_margin = sum(r['amount'] * fx.get(r['currency'], 1.0)
                           for _, r in margin_df.iterrows() if r['category'] == 'in_house')
        _futu_na = _futu_mv - _futu_margin
        _futu_ur = df.loc[df['broker'] == '富途', 'pnl_cny'].sum() if not df.empty else 0
        _futu_rp = (_closed_df.loc[_closed_df['broker'] == '富途', 'realized_pnl_cny'].sum()
                    if not _closed_df.empty else 0)
        _futu_residual = _futu_na - FUTU_CAPITAL - _futu_ur - _futu_rp
        _futu_int_fees = _futu_residual - _futu_fx_impact
        _futu_if_cls = _pnl_class(_futu_int_fees)

        st.markdown(f'''<div style="font-size:11px; color:var(--pf-text2); font-family:var(--pf-mono);
            margin-top:-8px; margin-bottom:16px; line-height:1.6;">
            Net P&L = Net Assets(¥{_fmt(_net_assets)}) − Capital(¥{_fmt(current_capital)})
            = <b class="{_tp_cls}">¥{_pnl_sign(_total_pl)}</b><br>
            Diff = Net P&L({_pnl_sign(_total_pl)})
            − Unrealized({_pnl_sign(_unrealized_pnl)})
            − Realized({_pnl_sign(_realized_pnl)})
            = <b class="{_fx_cls}">¥{_pnl_sign(_forex_gl)}</b><br>
            <span style="opacity:0.5;">* 其中 富途入金汇率影响:
            <b class="{_futu_fx_cls}">{_pnl_sign(_futu_fx_impact)}</b>
            (deposit@{FUTU_DEPOSIT_FX}→{fx.get("USD",0):.4f})<br>
            * 其中 富途融资利息及交易费:
            <b class="{_futu_if_cls}">{_pnl_sign(_futu_int_fees)}</b></span>
        </div>''', unsafe_allow_html=True)
    else:
        # ── Simplified P&L breakdown (cost mode) ──
        st.markdown(f'''<div style="font-size:11px; color:var(--pf-text2); font-family:var(--pf-mono);
            margin-top:-8px; margin-bottom:16px; line-height:1.6;">
            Net P&L = Net Assets(¥{_fmt(_net_assets)}) − Capital(¥{_fmt(current_capital)})
            = <b class="{_tp_cls}">¥{_pnl_sign(_total_pl)}</b><br>
            Unrealized: {_pnl_sign(_unrealized_pnl)} · Realized: {_pnl_sign(_realized_pnl)}
            · FX & Other: <b class="{_fx_cls}">{_pnl_sign(_forex_gl)}</b>
        </div>''', unsafe_allow_html=True)

    render_risk_analytics()
    render_attribution(df, fx)
    render_pnl_journal(df, fx)
    render_closed(fx)

    # Copyright
    st.markdown(
        '<div style="text-align:center;color:var(--pf-text2);font-size:11px;'
        'font-family:var(--pf-mono);padding:32px 0 16px;opacity:0.5;">'
        '© 2026 Alan He. All rights reserved.</div>',
        unsafe_allow_html=True,
    )


main()
