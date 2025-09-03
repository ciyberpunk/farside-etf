# eth_farside_to_csv.py
# Pulls ETH ETF daily flow table from Farside and writes clean CSVs
# to both Data/ and docs/Data/.
# NOTE: For ETH, we ALWAYS recompute "Total" as the sum of all fund columns.

import io
import re
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

ETH_URL = "https://farside.co.uk/ethereum-etf-flow-all-data/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Columns we never include in the Total sum:
EXCLUDE_COLS = {
    "date", "total", "eth", "average", "maximum", "minimum",
    "cumulative", "cumulative_usd_millions"
}

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "Data"
DOCS_DATA_DIR = REPO_ROOT / "docs" / "Data"


# ------------------------- Helpers -------------------------

def _norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ").replace("\u2009", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_cols(cols: List[str]) -> List[str]:
    return [_norm(c) for c in cols]


def _clean_num(x):
    """Convert table cell to float, handling commas and accounting negatives (parentheses)."""
    if pd.isna(x):
        return 0.0
    s = str(x).strip().replace(",", "").replace("\u2009", "").replace("\xa0", " ")
    if s in {"", "-", "–", "—"}:
        return 0.0
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_tables_with_pandas(html_fragment: str) -> List[pd.DataFrame]:
    """Return DataFrames for a single <table> element using pandas.read_html with two parsers."""
    dfs: List[pd.DataFrame] = []
    for flavor in ("lxml", "html5lib"):
        try:
            found = pd.read_html(io.StringIO(html_fragment), flavor=flavor, thousands=",")
            for df in found:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [
                        " ".join([str(x) for x in tup if str(x) != "nan"]).strip()
                        for tup in df.columns
                    ]
                df.columns = _norm_cols(list(df.columns))
                dfs.append(df)
            if dfs:
                break
        except Exception:
            continue
    return dfs


def _find_date_col(df: pd.DataFrame) -> Optional[str]:
    """Return the name of the column that holds dates. Prefer 'Date', else detect by parsing."""
    for cand in df.columns:
        if cand.lower().strip() == "date":
            return cand
    # Fallback: pick any column where most values parse as dates
    best_col, best_ratio = None, 0.0
    for c in df.columns:
        ser = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
        ratio = ser.notna().mean()
        if ratio > best_ratio:
            best_col, best_ratio = c, ratio
    if best_ratio > 0.6:
        return best_col
    return None


def _score_table(df: pd.DataFrame) -> tuple[int, int]:
    """Score a candidate table: more rows and more numeric-like columns are better."""
    n_rows = len(df)
    n_num_cols = 0
    for c in df.columns:
        if c.lower().strip() == "date":
            continue
        # Heuristic: if after cleaning, many cells look numeric
        try:
            vals = pd.Series(df[c]).map(_clean_num)
            if (vals != 0).mean() > 0.2:
                n_num_cols += 1
        except Exception:
            pass
    return (n_rows, n_num_cols)


def _pick_main_daily_table(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """Among all <table> elements, pick the one most likely to be the daily flow table."""
    best, best_score = None, (-1, -1)
    for tbl in soup.find_all("table"):
        for df in _parse_tables_with_pandas(str(tbl)):
            date_col = _find_date_col(df)
            if not date_col:
                continue
            score = _score_table(df)
            if score > best_score:
                best, best_score = df, score
    return best


def _load_raw_table(url: str = ETH_URL) -> pd.DataFrame:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    r = s.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    df = _pick_main_daily_table(soup)
    if df is None:
        raise RuntimeError("Could not find the main daily table on Farside (no date-like column).")
    return df


# ----------------------- Tidy / Build -----------------------

def _tidy_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Identify and normalize date column
    date_col = _find_date_col(df)
    if not date_col:
        raise RuntimeError("Found table but couldn't detect a date column.")
    df["date"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
    df = df[df["date"].notna()].drop(columns=[date_col])

    # Numeric conversion
    for c in list(df.columns):
        if c == "date":
            continue
        df[c] = pd.Series(df[c]).map(_clean_num)

    # Always recompute Total for ETH as the sum of fund columns
    fund_cols = [c for c in df.columns if c.lower() not in EXCLUDE_COLS and c != "date"]
    if not fund_cols:
        raise RuntimeError("No fund columns detected to sum for Total.")
    df["Total"] = df[fund_cols].sum(axis=1)

    # Sort by date ascending
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _force_daily_zero_fill(df_wide: pd.DataFrame) -> pd.DataFrame:
    full = pd.date_range(df_wide["date"].min(), df_wide["date"].max(), freq="D")
    df = (
        df_wide.set_index("date")
        .reindex(full)
        .fillna(0.0)
        .rename_axis("date")
        .reset_index()
    )
    return df


def build_outputs() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw = _load_raw_table(ETH_URL)
    wide = _tidy_wide(raw)
    wide = _force_daily_zero_fill(wide)

    # totals-only (plus cumulative)
    totals = wide[["date", "Total"]].rename(columns={"Total": "total_usd_millions"}).copy()
    totals["cumulative_usd_millions"] = totals["total_usd_millions"].cumsum()

    # long (tidy)
    melt_cols = [c for c in wide.columns if c not in {"date", "Total"}]
    long_ = wide.melt(
        id_vars="date", value_vars=melt_cols, var_name="fund", value_name="flow_usd_millions"
    )

    return wide, long_, totals


# ------------------------- I/O -------------------------

def _write_both_paths(df: pd.DataFrame, rel_name: str):
    """Write a CSV into both Data/ and docs/Data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    p1 = DATA_DIR / rel_name
    p2 = DOCS_DATA_DIR / rel_name

    df.to_csv(p1, index=False)
    df.to_csv(p2, index=False)
    print(f"Wrote: {p1.relative_to(REPO_ROOT)}  and  {p2.relative_to(REPO_ROOT)}")


def write_csvs():
    wide, long_, totals = build_outputs()

    # Format dates as YYYY-MM-DD for clean CSVs
    for df in (wide, long_, totals):
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    _write_both_paths(wide, "ethereum_etf_flows_wide_daily.csv")
    _write_both_paths(long_, "ethereum_etf_flows_long_daily.csv")
    _write_both_paths(totals, "ethereum_etf_totals_daily.csv")


if __name__ == "__main__":
    write_csvs()
