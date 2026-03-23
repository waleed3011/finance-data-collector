import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# ── CONFIGURE YOUR FUNDS HERE ──────────────────────────────────────────────
TICKERS = {
    "EUNL.DE":  "iShares Core MSCI World",
    "IS3N.DE":  "iShares EM IMI",
    "IUSN.DE":  "iShares MSCI World Small Cap",
    "VWCE.DE":  "Vanguard FTSE All-World",
    # Add your own tickers here...
}
# ───────────────────────────────────────────────────────────────────────────

OUTPUT_FILE = "data/fund_prices.csv"

def fetch_prices():
    rows = []
    today = datetime.today().strftime("%Y-%m-%d")

    for ticker, name in TICKERS.items():
        try:
            data = yf.Ticker(ticker)
            hist = data.history(period="1d")

            if hist.empty:
                print(f"  ⚠️  No data for {ticker}")
                continue

            close_price = round(hist["Close"].iloc[-1], 4)
            currency    = data.info.get("currency", "N/A")

            rows.append({
                "Date":     today,
                "Ticker":   ticker,
                "Name":     name,
                "Price":    close_price,
                "Currency": currency,
            })
            print(f"  ✅ {ticker}: {close_price} {currency}")

        except Exception as e:
            print(f"  ❌ Error fetching {ticker}: {e}")

    return pd.DataFrame(rows)

def update_csv(new_data: pd.DataFrame):
    os.makedirs("data", exist_ok=True)

    if os.path.exists(OUTPUT_FILE):
        existing = pd.read_csv(OUTPUT_FILE)
        # Avoid duplicate entries for same date + ticker
        combined = pd.concat([existing, new_data])
        combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
        combined = combined.sort_values(["Ticker", "Date"])
    else:
        combined = new_data

    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"\n📄 Saved {len(combined)} total rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    print(f"🚀 Collecting prices — {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    df = fetch_prices()
    if not df.empty:
        update_csv(df)
    else:
        print("No data collected.")
