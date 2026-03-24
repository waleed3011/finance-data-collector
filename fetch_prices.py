import yfinance as yf
import pandas as pd
from datetime import date
import os
os.makedirs("data", exist_ok=True)

# --- CONFIGURE YOUR TICKERS HERE ---
TICKERS = [
    "EURNOK=X", # EUR/NOK
    "NOK=X", # USD/NOK
    "SEKNOK=X", #SEK/NOK
    "DKKNOK=X", #DKK/NOK
    "0P00018CRD.IR", # ODIN Kreditt D
    "0P0001OPC5.IR", # KLP AksjeVerden Indeks N
    "0P0001OPBL.IR", # KLP AksjeGlobal Indeks N
    "0P0001RFXW.IR", # Heimdal Utbytte N
    "0P00000ODE.IR", # DNB SMB A
    "0P00000SFV.F", # Danske Invest Europe Hgh Dvd A
    "0P0001OPBE.IR", # KLP AksjeFremvoksende Markeder Indeks N
    "0P0001KO97.IR", # Storebrand Global Obligasjon N
]

START_DATE = "2023-01-01"
END_DATE = date.today().isoformat()

def fetch_data(tickers, start, end):
    all_frames = []
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            df = df[["Close"]].copy()
            df.columns = ["Close"]
            df["Ticker"] = ticker
            df.index.name = "Date"
            df.reset_index(inplace=True)
            all_frames.append(df)
        except Exception as e:
            print(f"Failed to fetch {ticker}: {e}")
    
    if not all_frames:
        raise ValueError("No data fetched")
    
    combined = pd.concat(all_frames, ignore_index=True)
    combined["Date"] = combined["Date"].dt.strftime("%Y-%m-%d")
    return combined

def main():
    df = fetch_data(TICKERS, START_DATE, END_DATE)

    df["Date"] = pd.to_datetime(df["Date"])

    # Build a complete date spine from START_DATE to END_DATE
    all_dates = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
    all_tickers = df["Ticker"].unique()

    # Create a full grid: every date x every ticker
    spine = pd.MultiIndex.from_product([all_dates, all_tickers], names=["Date", "Ticker"])
    spine_df = pd.DataFrame(index=spine).reset_index()

    # Merge actual data onto the full grid
    df = spine_df.merge(df, on=["Date", "Ticker"], how="left")

    # Forward fill per ticker, only after its first valid value
    df = df.sort_values(["Ticker", "Date"])
    df["Close"] = df.groupby("Ticker")["Close"].transform(lambda x: x.ffill())

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    # Long format — drop rows that are still NaN (i.e. before listing date)
    long = df.dropna(subset=["Close"])
    long.to_csv("data/prices_long.csv", index=False)

    # Wide format
    wide = df.pivot(index="Date", columns="Ticker", values="Close")
    wide.to_csv("data/prices_wide.csv")

    print(f"Exported {len(long)} rows across {long['Ticker'].nunique()} tickers")
    print(f"Date range: {long['Date'].min()} → {long['Date'].max()}")

if __name__ == "__main__":
    main()
