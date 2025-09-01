# visualize_totals.py
# Read totals CSVs from Data/ and create bar+line charts into Data/Charts/.
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# --- Locations (kept simple and stable) ---
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "Data"
OUT_DIR = DATA_DIR / "Charts"

FILES = [
    ("bitcoin", DATA_DIR / "bitcoin_etf_totals_daily.csv"),
    ("ethereum", DATA_DIR / "ethereum_etf_totals_daily.csv"),
]

def load_totals(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Defensive: normalize column names we expect from your generator scripts
    # Required: date, total_usd_millions; Optional: cumulative_usd_millions
    cols = {c.lower(): c for c in df.columns}

    if "date" not in cols:
        raise ValueError(f"'date' column not found in {csv_path.name}")

    # Prefer a strict parse; your files are YYYY-MM-DD
    df[cols["date"]] = pd.to_datetime(df[cols["date"]], format="%Y-%m-%d", errors="coerce")
    if df[cols["date"]].isna().any():
        # fallback if needed
        df[cols["date"]] = pd.to_datetime(df[cols["date"]], errors="coerce")

    # Total column may already be named 'total_usd_millions'; if not, try 'total'
    total_col = cols.get("total_usd_millions") or cols.get("total")
    if not total_col:
        raise ValueError(f"'total_usd_millions' (or 'Total') column not found in {csv_path.name}")

    df = df.rename(columns={total_col: "total_usd_millions", cols["date"]: "date"})

    # If cumulative not present, build it
    if "cumulative_usd_millions" not in df.columns:
        df["cumulative_usd_millions"] = df["total_usd_millions"].cumsum()

    # Sort by date just in case
    df = df.sort_values("date").reset_index(drop=True)
    return df[["date", "total_usd_millions", "cumulative_usd_millions"]]

def plot_asset(name: str, df: pd.DataFrame):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Split daily flows into positive (inflow) and negative (outflow) parts
    pos = df["total_usd_millions"].clip(lower=0)
    neg = df["total_usd_millions"].clip(upper=0)

    fig, ax = plt.subplots(figsize=(12, 6))

    # Bars: inflow (blue), outflow (red)
    ax.bar(df["date"], pos, width=0.85, color="#1f77b4", label="Daily inflow")  # blue
    ax.bar(df["date"], neg, width=0.85, color="#d62728", label="Daily outflow")  # red
    ax.set_ylabel("USD millions (daily)")

    # Line: cumulative on a secondary axis
    ax2 = ax.twinx()
    ax2.plot(
        df["date"],
        df["cumulative_usd_millions"],
        label="Cumulative (rhs)",
        color="black",
        linewidth=1.8,
        zorder=3,
    )
    ax2.set_ylabel("USD millions (cumulative)")

    # Title, grid, ticks
    ax.set_title(f"{name.upper()} spot ETF — daily flows (bars) & cumulative (line)")
    ax.grid(True, axis="y", alpha=0.3)

    # Nice date formatting
    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    # Combine legends from both axes
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")

    fig.tight_layout()
    out_path = OUT_DIR / f"{name}_totals_chart.png"
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"Saved: {out_path}")

def main():
    made_any = False
    for name, csv_path in FILES:
        if not csv_path.exists():
            print(f"Skipping {name}: {csv_path.name} not found in {csv_path.parent}")
            continue
        df = load_totals(csv_path)
        plot_asset(name, df)
        made_any = True

    if not made_any:
        print("No charts created — expected totals CSVs in the Data/ folder.")

if __name__ == "__main__":
    main()
