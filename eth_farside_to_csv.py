# eth_farside_to_csv.py
# Pulls ETH ETF daily flow table from Farside and writes clean CSVs.
# Doesn't scrape the site's "Total" column; computes Total by summing fund columns.
import re
import sys
from pathlib import Path
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

ETH_URL = "https://farside.co.uk/ethereum-etf-flow-all-data/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Expected ETH tickers in column order on Farside (left-to-right after the date)
ETH_TICKERS: List[str] = [
    "ETHA",  # BlackRock
    "FETH",  # Fidelity
    "ETHW",  # Bitwise
    "TETH",  # 21Shares
    "ETHV",  # VanEck
    "QETH",  # Invesco
    "EZET",  # Franklin
    "ETHE",  # Grayscale (Spot ETF)
    "ETH",   # Grayscale (second column on the site)
]

# Where to save
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"
DATA_DIR.mkdir(exist_ok=True)

# ---------- helpers ----------

DATE_RE = re.compile(r"^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$")  # e.g., "11 Jan 2024"

def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\xa0", " ").replace("\u2009", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _to_float(x) -> float:
    """Clean strings like '1,234.5', '(12.3)', '-', '—' into floats."""
    if x is None:
        return 0.0
    s = _norm_text(x)
    if s in {"", "-", "–", "—"}:
        return 0.0
    # strip commas / thin spaces / non-break spaces
    s = s.replace(",", "").replace("\u2009", "").replace("\xa0", "")
    # remove trailing footnote markers like '*'
    s = s.rstrip("*")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return 0.0

def _fetch_html(url: str) -> str:
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def _pick_eth_table(soup: BeautifulSoup):
    """
    Return the <table> that contains most of the ETH tickers.
    The ETH page doesn't always label the first column 'Date', so don't depend on that.
    """
    best_tbl, best_hits = None, -1
    for tbl in soup.find_all("table"):
        text = _norm_text(tbl.get_text(" "))
        hits = sum(1 for t in ETH_TICKERS if t in text)
        if hits > best_hits:
            best_tbl, best_hits = tbl, hits
    return best_tbl

def _table_rows(tbl) -> List[List[str]]:
    rows = []
    for tr in tbl.find_all("tr"):
        cells = [ _norm_text(td.get_text()) for td in tr.find_all(["th","td"]) ]
        if cells:
            rows.append(cells)
    return rows

def _parse_eth_daily_rows(rows_2d: List[List[str]]) -> pd.DataFrame:
    """
    Build a DataFrame with columns: ['date'] + ETH_TICKERS
    We ignore any 'Fee'/'Seed' rows and any trailing 'Total' cell from the site.
    """
    parsed = []
    for r in rows_2d:
        if not r:
            continue
        first = _norm_text(r[0])
        if not DATE_RE.match(first):
            continue  # skip header/fee/seed/etc.
        # After the date, the next cells are the nine tickers; site may also have a trailing "Total" — ignore it.
        # Ensure we have at least date + nine numbers:
        if len(r) < 1 + len(ETH_TICKERS):
            # Some pages compress cells; skip incomplete rows
            continue
        vals = r[1 : 1 + len(ETH_TICKERS)]
        parsed.append([first] + vals)

    if not parsed:
        raise RuntimeError("No daily rows found (couldn't locate any 'DD Mon YYYY' dates).")

    cols = ["date"] + ETH_TICKERS
    df = pd.DataFrame(parsed, columns=cols)

    # Convert date and numerics
    df["date"] = pd.to_datetime(df["date"], format="%d %b %Y", errors="coerce")
    for c in ETH_TICKERS:
        df[c] = df[c].map(_to_float)

    # drop any bad date rows (shouldn't happen) and sort ascending
    df = df[df["date"].notna()].sort_values("date").reset_index(drop=True)
    return df

def _force_daily_zero_fill(df_wide: pd.DataFrame) -> pd.DataFrame:
    full = pd.date_range(df_wide["date"].min(), df_wide["date"].max(), freq="D")
    out = (
        df_wide.set_index("date")
               .reindex(full)
               .fillna(0.0)
               .rename_axis("date")
               .reset_index()
    )
    return out

# ---------- main pipeline ----------

def build_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    html = _fetch_html(ETH_URL)
    soup = BeautifulSoup(html, "lxml")

    tbl = _pick_eth_table(soup)
    if tbl is None:
        raise RuntimeError("Couldn't find an ETH ETF table on the page.")

    rows = _table_rows(tbl)
    wide = _parse_eth_daily_rows(rows)

    # Compute Total as the sum of fund columns (do NOT read site total)
    wide["Total"] = wide[ETH_TICKERS].sum(axis=1)

    # Fill to daily calendar (zeros on missing dates)
    wide = _force_daily_zero_fill(wide)

    # Build long/totals outputs
    totals = (
        wide[["date", "Total"]]
        .rename(columns={"Total": "total_usd_millions"})
        .copy()
    )
    totals["cumulative_usd_millions"] = totals["total_usd_millions"].cumsum()

    long_ = wide.melt(
        id_vars="date",
        value_vars=ETH_TICKERS,  # long form is funds only
        var_name="fund",
        value_name="flow_usd_millions",
    )

    return wide, long_, totals

def write_csvs():
    wide, long_, totals = build_outputs()

    # Friendly date format
    for df in (wide, long_, totals):
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    (DATA_DIR / "ethereum_etf_flows_wide_daily.csv").write_text(
        wide.to_csv(index=False), encoding="utf-8"
    )
    (DATA_DIR / "ethereum_etf_flows_long_daily.csv").write_text(
        long_.to_csv(index=False), encoding="utf-8"
    )
    (DATA_DIR / "ethereum_etf_totals_daily.csv").write_text(
        totals.to_csv(index=False), encoding="utf-8"
    )

    print("Wrote:")
    print(f"  - {DATA_DIR / 'ethereum_etf_flows_wide_daily.csv'}")
    print(f"  - {DATA_DIR / 'ethereum_etf_flows_long_daily.csv'}")
    print(f"  - {DATA_DIR / 'ethereum_etf_totals_daily.csv'}")

if __name__ == "__main__":
    try:
        write_csvs()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
