# Automated Earthquake Data Pipeline (Airflow + AWS S3 + MotherDuck + Looker Studio)

An end-to-end, orchestrated, and containerized data pipeline built with **Apache Airflow**, **AWS S3**, **MotherDuck (Cloud DuckDB)**, and **Looker Studio**. 

The pipeline ingests real-time seismic event data from the USGS API, stores raw JSON files in an AWS S3 Data Lake (**Bronze Layer**), flattens the nested structures using Pandas into MotherDuck (**Silver Layer**), generates SQL analytics (**Gold Layer**), and powers an interactive geospatial dashboard in **Looker Studio**.

<img width="1442" height="273" alt="image" src="https://github.com/user-attachments/assets/4f872ee5-78dc-417c-aa2a-6ac6d4dc06ba" />

---

## — Pipeline Architecture

```text
               [ USGS Earthquake API ] (GeoJSON)
                          │
                          ▼
             [ Apache Airflow (Docker) ]
             └──► extract_earthquake_data
                         │
        ┌────────────────┴────────────────┐
        ▼                                 ▼
 [ upload_to_s3 ]           [ load_to_motherduck_silver ]
   (AWS S3 Data Lake)          (MotherDuck Cloud DWH)
   Bronze Layer                Silver Layer: silver_earthquakes
   raw/earthquakes_*.json                 │
                                          ▼
                               [ create_gold_layer ]
                                 MotherDuck Gold Layer
                                 gold_high_intensity_events
                                          │
                                          ▼
                               [ Looker Studio Dashboard ]
                                 Geospatial & Risk Analytics
```

---

## — Tech Stack & Software Practices

- **Workflow Orchestration:** Apache Airflow 2.x (TaskFlow API `@dag`, `@task`).
- **Cloud Storage (Data Lake):** AWS S3 (Bronze Layer auditing).
- **Data Warehouse:** MotherDuck & DuckDB (Silver & Gold Layers querying).
- **Data Transformation:** Python 3.12, Pandas & DuckDB SQL (JSON flattening & epoch timestamp conversion).
- **Business Intelligence:** Looker Studio (Geospatial Google Maps & Risk Analytics).
- **Containerization:** Astro CLI / Docker Compose.
- **Security & Governance:** Airflow Connections (`aws_default`) & Airflow Variables (`motherduck_token`) to prevent credential hardcoding.

---

## — Database Schema (MotherDuck)

### Silver Layer (`silver_earthquakes`)
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `id` | STRING | Unique event identifier from USGS |
| `magnitude` | FLOAT | Richter scale magnitude |
| `place` | STRING | Geographic location description |
| `timestamp` | TIMESTAMP | Event date and time (converted from ms epoch) |
| `longitude` | FLOAT | Geographic longitude coordinate |
| `latitude` | FLOAT | Geographic latitude coordinate |
| `depth` | FLOAT | Earthquake depth in kilometers |

---

## — Interactive BI Dashboard (Looker Studio)

The pipeline powers a live geospatial and seismic activity dashboard connected directly to MotherDuck via PostgreSQL wire protocol.

👉 **[Click here to view the Live Looker Studio Dashboard](https://datastudio.google.com/reporting/6c250ed8-c2fe-491b-a6ec-dcb85ad03007)**

---

## — How to Run This Project

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/) installed and running.
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed.
- AWS Account (S3 Bucket: `earthquake-data-bronze`) & IAM Credentials.
- MotherDuck Account & Access Token.

### 2. Setup Airflow Environment
Clone this repository and spin up the containerized environment:

```bash
git clone https://github.com/karenymorel/earthquake-pipeline.git
cd earthquake-pipeline
astro dev start
```

### 3. Configure Credentials in Airflow UI (`http://localhost:8080`)
1. **AWS Connection:** Go to **Admin ➔ Connections**, create `aws_default` (Type: *Amazon Web Services*) and input your `AWS Access Key ID` and `AWS Secret Access Key`.
2. **MotherDuck Token:** Go to **Admin ➔ Variables**, create key `motherduck_token` and paste your MotherDuck access token.

### 4. Execute the Pipeline
Toggle the `earthquake_api_to_s3_v1` DAG to **ON** and click **Trigger DAG**.
