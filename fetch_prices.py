import yfinance as yf
import pandas as pd
from datetime import date
import os
os.makedirs("data", exist_ok=True)

# --- CONFIGURE TICKERS ---
CURRENCY_TICKERS = [
    "EURNOK=X",   # EUR/NOK
    "NOK=X",      # USD/NOK
    "SEKNOK=X",   # SEK/NOK
    "DKKNOK=X",   # DKK/NOK
]

ASSET_TICKERS = [
    "0P00018CRD.IR",  # ODIN Kreditt D
    "0P0001OPC5.IR",  # KLP AksjeVerden Indeks N
    "0P0001OPBL.IR",  # KLP AksjeGlobal Indeks N
    "0P0001RFXW.IR",  # Heimdal Utbytte N
    "0P00000ODE.IR",  # DNB SMB A
    "0P00000SFV.F",   # Danske Invest Europe Hgh Dvd A
    "0P0001OPBE.IR",  # KLP AksjeFremvoksende Markeder Indeks N
    "0P0001KO97.IR",  # Storebrand Global Obligasjon N
]

START_DATE = "2023-01-01"
END_DATE = date.today().isoformat()


def fetch_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Fetch closing prices for a list of tickers, returned as a wide DataFrame."""
    frames = {}
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            if df.empty:
                print(f"No data for {ticker}")
                continue
            frames[ticker] = df["Close"].squeeze()
        except Exception as e:
            print(f"Failed to fetch {ticker}: {e}")

    if not frames:
        raise ValueError("No data fetched")

    wide = pd.DataFrame(frames)
    wide.index = pd.to_datetime(wide.index).normalize()
    wide.index.name = "Date"
    return wide


def forward_fill_after_listing(wide: pd.DataFrame) -> pd.DataFrame:
    """
    For each ticker, only forward-fill gaps AFTER its first known value.
    Rows before the listing date remain NaN.
    """
    filled = wide.copy()
    for col in filled.columns:
        first_valid = filled[col].first_valid_index()
        if first_valid is not None:
            filled.loc[first_valid:, col] = filled.loc[first_valid:, col].ffill()
    return filled


def to_long(wide: pd.DataFrame) -> pd.DataFrame:
    """Convert wide DataFrame to long format with Date, Ticker, Close columns."""
    long = wide.reset_index().melt(id_vars="Date", var_name="Ticker", value_name="Close")
    long = long.dropna(subset=["Close"])
    long["Date"] = long["Date"].dt.strftime("%Y-%m-%d")
    long = long.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    return long


def export(wide: pd.DataFrame, prefix: str):
    """Export both wide and long CSVs for a given dataset."""
    wide_export = wide.copy()
    wide_export.index = wide_export.index.strftime("%Y-%m-%d")

    wide_export.to_csv(f"data/{prefix}_wide.csv")

    long = to_long(wide)
    long.to_csv(f"data/{prefix}_long.csv", index=False)

    print(f"[{prefix}] Exported {len(long)} long rows, {len(wide_export)} dates, {len(wide.columns)} tickers")


def main():
    os.makedirs("data", exist_ok=True)

    print("Fetching currencies...")
    currencies_wide = fetch_prices(CURRENCY_TICKERS, START_DATE, END_DATE)
    currencies_wide = forward_fill_after_listing(currencies_wide)
    export(currencies_wide, "currencies")

    print("Fetching assets...")
    assets_wide = fetch_prices(ASSET_TICKERS, START_DATE, END_DATE)
    assets_wide = forward_fill_after_listing(assets_wide)
    export(assets_wide, "prices")


if __name__ == "__main__":
    main()
