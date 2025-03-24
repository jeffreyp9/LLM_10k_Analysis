# This script scrapes the S&P 500 Wikipedia page for Ticker, Company, and CIK
# It then fetches MarketCap from Yahoo Finance
# It sorts by descending MarketCap
# And saves Ticker, Company, CIK, MarketCap to CSV

# import libraries
import pandas as pd
import yfinance as yf
import time

# define wiki url and output csv
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
OUTPUT_CSV = "sp500_companies.csv"

# function to fetch market cap
def fetch_market_cap(ticker):
    """
    Uses yfinance to fetch 'marketCap'.
    Returns 0 if there's any issue or if the field doesn't exist.
    """
    try:
        info = yf.Ticker(ticker).info
        return info.get("marketCap", 0) or 0
    except:
        return 0

# function to scrape S&P 500 and market cap to csv
def scrape_sp500_and_marketcap_to_csv():
    """
    1) Scrapes the S&P 500 Wikipedia page for Ticker, Company, and CIK
    2) Fetches MarketCap from Yahoo Finance
    3) Sorts by descending MarketCap
    4) Saves Ticker, Company, CIK, MarketCap to CSV
    """
    # 1) Read all tables from the wiki page
    df_list = pd.read_html(WIKI_URL)
    df_sp500 = df_list[0]

    # 2) Rename columns for clarity
    df_sp500.rename(
        columns={"Symbol": "Ticker", "Security": "Company"},
        inplace=True
    )

    # 3) Zero-pad the 'CIK' column if it exists
    if "CIK" in df_sp500.columns:
        df_sp500["CIK"] = (
            df_sp500["CIK"]
            .fillna("")
            .astype(str)
            .apply(lambda x: x.zfill(10) if x else "")
        )
    else:
        df_sp500["CIK"] = ""

    # keep only Ticker, Company, CIK
    df_sp500 = df_sp500[["Ticker", "Company", "CIK"]]

    # Some S&P 500 tickers may contain a "." (e.g. "BRK.B", "BF.B")
    # yfinance often expects a "-" instead of "."
    # So let's convert them, but store *both* the original Ticker and the "yf_ticker"
    df_sp500["yf_Ticker"] = df_sp500["Ticker"].replace({r"\.": "-"}, regex=True)

    # For each row, fetch the market cap from yfinance
    market_caps = []
    for idx, row in df_sp500.iterrows():
        ticker_for_yf = row["yf_Ticker"]
        market_cap = fetch_market_cap(ticker_for_yf)
        market_caps.append(market_cap)
        # (Optional) Sleep a bit to be polite to Yahoo
        time.sleep(0.2)  # 200ms per ticker

    df_sp500["MarketCap"] = market_caps

    # Sort by descending MarketCap
    df_sp500.sort_values(by="MarketCap", ascending=False, inplace=True)

    # Save only Ticker, Company, CIK, MarketCap in the final CSV
    df_sp500.to_csv(OUTPUT_CSV, columns=["Ticker", "Company", "CIK", "MarketCap"], index=False)

    print(f"Saved {len(df_sp500)} rows to {OUTPUT_CSV} (sorted by MarketCap desc)")

if __name__ == "__main__":
    scrape_sp500_and_marketcap_to_csv()