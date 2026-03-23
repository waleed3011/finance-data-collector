import yfinance as yf
import pandas as pd
import os

# ── SAME TICKERS AS YOUR MAIN SCRIPT ──────────────────────────────────────
TICKERS = {
    "EUNL.DE":  "iShares Core MSCI World",
    "IS3N.DE":  "iShares EM IMI",
    "IUSN.DE":  "iShares MSCI World Small Cap",
    "VWCE.DE":  "Vanguard FTSE All-World",
    # Add your funds here...
}
# ───────────────────────────────────────────────────────────────────────────

START_DATE  = "2025-01-01"
END_DATE    = "today"          # or e.g. "2025-03-23"
OUTPUT_FILE = "data/fund_prices.csv"

def backfill():
    os.makedirs("data", exist_ok=True)
    all_rows = []

    for ticker, name in TICKERS.items():
        print(f"Fetching {ticker}...")
        try:
            data = yf.Ticker(ticker)
            hist = data.history(start=START_DATE, end=END_DATE)
            currency = data.info.get("currency", "N/A")

            if hist.empty:
                print(f"  ⚠️  No data for {ticker}")
                continue

            for date, row in hist.iterrows():
                all_rows.append({
                    "Date":     date.strftime("%Y-%m-%d"),
                    "Ticker":   ticker,
                    "Name":     name,
                    "Price":    round(row["Close"], 4),
                    "Currency": currency,
                })
            print(f"  ✅ {len(hist)} rows fetched")

        except Exception as e:
            print(f"  ❌ Error: {e}")

    new_df = pd.DataFrame(all_rows)

    # Merge with existing CSV if it exists
    if os.path.exists(OUTPUT_FILE):
        existing = pd.read_csv(OUTPUT_FILE)
        combined = pd.concat([existing, new_df])
        combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
        combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    else:
        combined = new_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Done! {len(combined)} total rows saved to {OUTPUT_FILE}")
    print(combined.head(10))

if __name__ == "__main__":
    backfill()
