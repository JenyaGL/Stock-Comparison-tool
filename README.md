# Stock-Comparison-tool
its end to end pipeline that ingests free stock market data from Finnhub(or any other free API provider), stores raw messages in GCS (Google Cloud Storage), loads them into BigQuery raw tables, transforms them to BI-ready fact/dimension tables (daily OHLC + KPIs), and visualizes comparisons of several stocks simultaneously in Power BI.
