import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# ── CONFIGURE YOUR TICKERS HERE — only edit this section ──────────────────
TICKERS = {
    "EUNL.DE":  "iShares Core MSCI World",
    "IS3N.DE":  "iShares EM IMI",
    "IUSN.DE":  "iShares MSCI World Small Cap",
    "VWCE.DE":  "Vanguard FTSE All-World",
    # Just add new tickers here — nothing else needs to change
}

# Date to fetch FROM when no existing data is found for a ticker
DEFAULT_START_DATE = "2025-01-01"
# ───────────────────────────────────────────────────────────────────────────

OUTPUT_FILE = "data/fund_prices.csv"
TODAY       = datetime.today().strftime("%Y-%m-%d")


def load_existing() -> pd.DataFrame:
    """Load existing CSV if it exists, otherwise return empty DataFrame."""
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, parse_dates=["Date"])
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        return df
    return pd.DataFrame(columns=["Date", "Ticker", "Name", "Price", "Currency"])


def get_start_date_for_ticker(ticker: str, existing: pd.DataFrame) -> str:
    """
    Smart start date logic:
    - If ticker already has data → fetch from day after last entry
    - If ticker is new → fetch from DEFAULT_START_DATE
    """
    ticker_data = existing[existing["Ticker"] == ticker]
    if ticker_data.empty:
        print(f"  📦 New ticker {ticker} — fetching from {DEFAULT_START_DATE}")
        return DEFAULT_START_DATE
    else:
        last_date = ticker_data["Date"].max()
        next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"  🔄 {ticker} — last entry {last_date}, fetching from {next_date}")
        return next_date


def fetch_ticker(ticker: str, name: str, start_date: str) -> pd.DataFrame:
    """Fetch price history for a single ticker from start_date to today."""
    if start_date > TODAY:
        print(f"  ✅ {ticker} already up to date")
        return pd.DataFrame()

    try:
        data  = yf.Ticker(ticker)
        hist  = data.history(start=start_date, end=TODAY)
        currency = data.info.get("currency", "N/A")

        if hist.empty:
            print(f"  ⚠️  No data returned for {ticker}")
            return pd.DataFrame()

        rows = []
        for date, row in hist.iterrows():
            rows.append({
                "Date":     date.strftime("%Y-%m-%d"),
                "Ticker":   ticker,
                "Name":     name,
                "Price":    round(row["Close"], 4),
                "Currency": currency,
            })

        print(f"  ✅ {ticker}: {len(rows)} new rows fetched")
        return pd.DataFrame(rows)

    except Exception as e:
        print(f"  ❌ Error fetching {ticker}: {e}")
        return pd.DataFrame()


def save(existing: pd.DataFrame, new_data: pd.DataFrame):
    """Merge new data with existing, deduplicate, sort, and save."""
    os.makedirs("data", exist_ok=True)

    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
    combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"\n💾 Saved {len(combined)} total rows to {OUTPUT_FILE}")


def main():
    print(f"🚀 Starting price collection — {TODAY}\n")

    existing = load_existing()
    all_new  = []

    for ticker, name in TICKERS.items():
        start_date = get_start_date_for_ticker(ticker, existing)
        new_df     = fetch_ticker(ticker, name, start_date)
        if not new_df.empty:
            all_new.append(new_df)

    if all_new:
        new_data = pd.concat(all_new, ignore_index=True)
        save(existing, new_data)
    else:
        print("\n✅ All tickers already up to date — nothing to commit")


if __name__ == "__main__":
    main()
