# I use this script to pull ticker data from Finnhub Stock API.
# you can add/remove tickers from the list Just dont forget to update the country dict.
# Please nore that there is a limit of 60 pulls per minute so DONT add more than 60 tickers.
# in this version i have only 19 tickers.

# -------------------------------------------

# libs:
import os
import pip
## pip install google-cloud-storage requests
import requests
import json
from datetime import datetime
from google.cloud import storage




# CONFIGURATION:

API_KEY = '--'   # ← Replace with your real key

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "your json key from GCS"

GCS_BUCKET = 'Write the name of your GCS bucket'

CREDENTIAL_PATH = 'your json key from GCS'

FOLDER = 'write the folder name thats in the bucket'

# TICKERS LIST:                  
TICKERS = [
 # US
    'LMT',   # Lockheed Martin
    'RTX',   # RTX Corporation
    'NOC',   # Northrop Grumman
    'GD',    # General Dynamics
    'BA',    # Boeing

    # UK
    'BAESY', # BAE Systems ADR (U.S. traded)
    'RYCEY', # Rolls-Royce ADR
    'QNTQF', # QinetiQ (OTC)
    'BCKIF', # Babcock Intl (OTC)
    'AVNFF', # Avon Protection (London Stock Exchange — LSE access needed)

    # FR
    'SAFRF', # Safran (OTC)
    'THLEF', # Thales Group (OTC)
    # Dassault, MBDA, Naval Group → Not publicly traded / no active ticker

    # DE
    'RNMBF', # Rheinmetall (OTC)
    'HAGHY',# Hensoldt AG (Frankfurt)
    # MTX.DE, TKAG.DE → hard to verify defense relevance on free tier

    # IT
    'FINMY', # Fincantieri (OTC ADR)
    'AVIOY',# Avio (Borsa Italiana)

    # ISR
    'ESLT',  # Elbit Systems (NASDAQ)
    'RADA',  # RADA Electronic (now part of DRS)
    'IAI'    # Israel Aerospace Industries – not publicly listed
]

# Countries

TICKER_COUNTRY_MAP = {
    # US
    'LMT': 'USA', 'RTX': 'USA', 'NOC': 'USA', 'GD': 'USA', 'BA': 'USA',

    # UK
    'BAESY': 'UK', 'RYCEY': 'UK', 'QNTQF': 'UK', 'BCKIF': 'UK', 'AVNFF': 'UK',

    # France
    'SAFRF': 'France', 'THLEF': 'France',

    # Germany
    'RNMBF': 'Germany', 'HAGHY': 'Germany',

    # Italy
    'FINMY': 'Italy', 'AVIOY': 'Italy',

    # Israel
    'ESLT': 'Israel', 'RADA': 'Israel', 'IAI': 'Israel'
}


# AUTHENTICATE:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
client = storage.Client()
bucket = client.bucket(GCS_BUCKET)


# FUNCTION TO PULL & UPLOAD:
def fetch_and_upload(ticker):
    print(f"\n Starting process for: {ticker}")

    url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={API_KEY}'
    print(f"Requesting data from: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ API request failed for {ticker} with status {response.status_code}")
        return

    data = response.json()

    # ✅ Add required metadata
    data["symbol"] = ticker
    data["fetched_at"] = datetime.utcnow().isoformat()
    data["country"] = TICKER_COUNTRY_MAP.get(ticker, "Unknown")

    filename = f"{FOLDER}/{ticker}_{datetime.today().strftime('%Y-%m-%d')}.json"
    blob = bucket.blob(filename)

    ndjson_string = json.dumps(data) + "\n"  # this gives NDJSON format which is essintial to Big Query

    try:
        blob.upload_from_string(ndjson_string, content_type='application/json')
        print(f"📤 Uploaded to: gs://{GCS_BUCKET}/{filename}")
        print(f"🧾 Stored JSON: {ndjson_string}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")


# MAIN EXECUTION:
if __name__ == "__main__":
    print("🚀 Starting stock data pipeline...\n")
    for ticker in TICKERS:
        fetch_and_upload(ticker)
    print("\n✅ Pipeline run completed.")
