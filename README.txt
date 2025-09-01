This repo contains two Python scripts that scrape the daily ETF flow tables from Farside and write clean CSVs to the `Data/` folder.

- `btc_farside_to_csv.py` – Bitcoin ETFs
- `eth_farside_to_csv.py` – Ethereum ETFs
- Output CSVs land in `Data/`:
  - `<asset>_etf_flows_wide_daily.csv`
  - `<asset>_etf_flows_long_daily.csv`
  - `<asset>_etf_totals_daily.csv`

## Quick start

```bash
cd farside-etf
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python btc_farside_to_csv.py
python eth_farside_to_csv.py