# eth_farside_to_csv.py
# Pulls ETH ETF daily flow table from Farside and writes clean CSVs to Data/ and docs/Data/.
# IMPORTANT: For ETH, we ALWAYS recompute 'Total' as the sum of the fund columns (per your request).
import io
import re
import shutil
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://farside.co.uk/ethereum-etf-flow-all-data/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Folders
REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "Data"
DOCS_DATA_DIR = REPO_ROOT / "docs" / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Columns we never sum into Total if the site ever hides extras in the table
EXCLUDE_COLS = {"date", "total", "btc", "eth", "average", "maximum", "minimum"}

def _publish_to_pages(outputs):
    """Copy CSVs written to Data/ into docs/Data/ so GitHub Pages can serve them."""
    for src in outputs:
        dst = DOCS_DATA_DIR / src.name
        shutil.copy2(src, dst)

def _norm(s: str) -> str:
    s = ("" if s is None else str(s))
    s = s.replace("\xa0", " ").replace("\u2009", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_cols(cols: List[str]) -> List[str]:
    return [_norm(c) for c in cols]

def _clean_num(x):
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    s = s.replace(",", "").replace("\u2009", "").replace("\xa0", " ")
    if s in {"", "-", "–", "—"}:
        return 0.0
    if s.startswith("(") and s.endswith(")"):  # accounting negative
        s = "-" + s[1:-1]
    s = s.replace("−", "-")  # unicode minus
    try:
        return float(s)
    except Exception:
        return 0.0

def _parse_tables_with_pandas(html_fragment: str) -> List[pd.DataFrame]:
    dfs = []
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

def _pick_main_daily_table(soup: BeautifulSoup) -> pd.DataFrame | None:
    best, best_score = None, (-1, -1)
    for tbl in soup.find_all("table"):
        for df in _parse_tables_with_pandas(str(tbl)):
            if "Date" not in df.columns:
                continue
            # score: more rows + more numeric-like columns
            score = (len(df), sum(c != "Date" for c in df.columns))
            if score > best_score:
                best, best_score = df, score
    return best

def _load_raw_table() -> pd.DataFrame:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    r = s.get(URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    df = _pick_main_daily_table(soup)
    if df is None or "Date" not in df.columns:
        raise RuntimeError("Could not find the main daily table on Farside (no 'Date' header).")
    return df

def _tidy_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Keep only true daily rows (drop footer summary rows)
    df["date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df[df["date"].notna()].drop(columns=["Date"])

    # Convert values to floats (handle commas and (negatives))
    for c in list(df.columns):
        if c == "date":
            continue
        df[c] = df[c].map(_clean_num)

    # ALWAYS recompute 'Total' as the sum of fund columns (ignore any provided Total on page)
    sum_cols = [c for c in df.columns if c.lower() not in EXCLUDE_COLS and c != "date"]
    df["Total"] = df[sum_cols].sum(axis=1)

    # Sort by date (ascending)
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
    raw = _load_raw_table()
    wide = _tidy_wide(raw)
    wide = _force_daily_zero_fill(wide)

    # totals-only (plus cumulative)
    totals = wide[["date", "Total"]].rename(columns={"Total": "total_usd_millions"}).copy()
    totals["cumulative_usd_millions"] = totals["total_usd_millions"].cumsum()

    # long (tidy) — one row per date per fund (excluding the recomputed Total)
    melt_cols = [c for c in wide.columns if c not in {"date", "Total"}]
    long_ = wide.melt(
        id_vars="date", value_vars=melt_cols, var_name="fund", value_name="flow_usd_millions"
    )

    return wide, long_, totals

def write_csvs():
    wide, long_, totals = build_outputs()
    # Consistent date formatting
    for df in (wide, long_, totals):
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Write into Data/
    out_wide = DATA_DIR / "ethereum_etf_flows_wide_daily.csv"
    out_long = DATA_DIR / "ethereum_etf_flows_long_daily.csv"
    out_totals = DATA_DIR / "ethereum_etf_totals_daily.csv"

    wide.to_csv(out_wide, index=False)
    long_.to_csv(out_long, index=False)
    totals.to_csv(out_totals, index=False)

    # Mirror to docs/Data/ for GitHub Pages
    _publish_to_pages([out_wide, out_long, out_totals])

    print("Wrote:")
    print(f"  - {out_wide}")
    print(f"  - {out_long}")
    print(f"  - {out_totals}")
    print("Also copied to docs/Data for GitHub Pages.")

if __name__ == "__main__":
    write_csvs()
