# Stock-Comparison-tool

## Objective:
To build an end-to-end stock data pipeline leveraging a free market data API, Google Cloud Storage, BigQuery, and Looker.

The goal is to pull real-time and historical stock data via API, store it in cloud storage(Google cloud Storage), stage and transform it through structured SQL layers (Raw → Filtered → BI-ready) in Big Quqry, and visualize KPIs in Looker.

# Architecture

<img width="951" height="671" alt="image" src="https://github.com/user-attachments/assets/59fe4dfe-eaac-437e-aab8-86f4940359ef" />


-  Cloud: This stages stores the data from the API, the data is stored as is from its source.
  
-  Raw Layer: This stage stores data as-is from the cloud, this is where json files are ingested into a big query relational database.

-  Filtered Layer: This stage stores the already clean and normalized. in this layer we we prepare the data for analysis.

-  BI Ready Layer: This layer contains business-ready data modeled into a star schema required for reporting and analytics.
  
-  Looker Studio
  

  I specifically chose Looker Studio as the BI tool since the data is already being stored in GCS(Google cloud services) and processed with Big Query the only logical move is to stay within the google ecosystem and implement Looker into this project.

  [Looker Report](https://lookerstudio.google.com/s/v_q-FY5MVXI)
  
