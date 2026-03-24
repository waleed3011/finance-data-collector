import yfinance as yf
import pandas as pd
from datetime import date

# --- CONFIGURE YOUR TICKERS HERE ---
TICKERS = [
    "AAPL", "MSFT", "EQNR.OL",   # stocks
    "EUNL.DE", "IS3N.DE",          # ETFs
    # add all your holdings here
]

START_DATE = "2025-01-01"
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
    
    # Long format (one row per ticker per date) — best for Power Query
    df.to_csv("data/prices_long.csv", index=False)
    
    # Wide format (dates as rows, tickers as columns) — alternative
    wide = df.pivot(index="Date", columns="Ticker", values="Close")
    wide.to_csv("data/prices_wide.csv")
    
    print(f"Exported {len(df)} rows across {df['Ticker'].nunique()} tickers")
    print(f"Date range: {df['Date'].min()} → {df['Date'].max()}")

if __name__ == "__main__":
    main()
