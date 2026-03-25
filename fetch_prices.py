import yfinance as yf
import pandas as pd
from datetime import date
import requests
import io
import os

os.makedirs("data", exist_ok=True)

TICKERS = [
    "EURNOK=X",       # EUR/NOK
    "NOK=X",          # USD/NOK
    "SEKNOK=X",       # SEK/NOK
    "DKKNOK=X",       # DKK/NOK
    "0P00018CRD.IR",  # ODIN Kreditt D
    "0P0001OPC5.IR",  # KLP AksjeVerden Indeks N
    "0P0001OPBL.IR",  # KLP AksjeGlobal Indeks N
    "0P0001RFXW.IR",  # Heimdal Utbytte N
    "0P00000ODE.IR",  # DNB SMB A
    "0P00000SFV.F",   # Danske Invest Europe Hgh Dvd A
    "0P0001OPBE.IR",  # KLP AksjeFremvoksende Markeder Indeks N
    "0P0001KO97.IR",  # Storebrand Global Obligasjon N
    "0P0000J24W.ST",  # Nordnet Sverige Index
    "0P000134K9.F",   # Nordnet Suomi Indeksi
    "0P000134K7.IR",  # Nordnet Norge Indeks
    "0P0001K6NJ.IR",  # Nordnet Global Indeks NOK
    "0P0001K6NB.IR",  # Nordnet Emerging Markets Indeks
    "0P0001KRXX.CO",  # Nordnet Danmark Indeks B
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
    combined["Date"] = pd.to_datetime(combined["Date"])
    return combined


def fetch_norway_10y(start, end):
    """Fetch Norway 10-year government bond yield from Norges Bank open API."""
    url = (
        f"https://data.norges-bank.no/api/data/IR/B.GBON.10Y."
        f"?format=csv&startPeriod={start}&endPeriod={end}&locale=en"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Norges Bank CSV has a metadata header — skip lines starting with #
        lines = [l for l in response.text.splitlines() if not l.startswith("#")]
        clean_csv = "\n".join(lines)

        df = pd.read_csv(io.StringIO(clean_csv), sep=";")

        # Keep only date and value columns, rename to match our schema
        df = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
        df.columns = ["Date", "Close"]
        df["Date"] = pd.to_datetime(df["Date"])
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Ticker"] = "NO10Y"

        print(f"OK: NO10Y (Norges Bank) — {len(df)} rows")
        return df

    except Exception as e:
        print(f"Failed to fetch Norway 10Y yield: {e}")
        return None


def main():
    df = fetch_data(TICKERS, START_DATE, END_DATE)

    # Fetch Norway 10Y from Norges Bank and append
    nb_df = fetch_norway_10y(START_DATE, END_DATE)
    if nb_df is not None:
        df = pd.concat([df, nb_df], ignore_index=True)

    # Build a complete date spine
    all_dates = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
    all_tickers = df["Ticker"].unique()

    spine = pd.MultiIndex.from_product([all_dates, all_tickers], names=["Date", "Ticker"])
    spine_df = pd.DataFrame(index=spine).reset_index()

    df = spine_df.merge(df, on=["Date", "Ticker"], how="left")

    # Forward fill per ticker after its first valid value
    df = df.sort_values(["Ticker", "Date"])
    df["Close"] = df.groupby("Ticker")["Close"].transform(lambda x: x.ffill())

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    # Long format — drop pre-listing NaNs
    long = df.dropna(subset=["Close"])
    long.to_csv("data/prices_long.csv", index=False)

    # Wide format
    wide = df.pivot(index="Date", columns="Ticker", values="Close")
    wide.to_csv("data/prices_wide.csv")

    print(f"Exported {len(long)} rows across {long['Ticker'].nunique()} tickers")
    print(f"Date range: {long['Date'].min()} → {long['Date'].max()}")


if __name__ == "__main__":
    main()
