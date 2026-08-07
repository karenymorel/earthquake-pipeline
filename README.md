# Automated Earthquake Data Pipeline (Airflow + AWS S3 + MotherDuck)

An end-to-end, orchestrated, and containerized data pipeline built with **Apache Airflow**, **AWS S3**, and **MotherDuck (Cloud DuckDB)**. 

The pipeline ingests real-time seismic event data from the USGS API, stores raw JSON files in an AWS S3 Data Lake (**Bronze Layer**), flattens the nested structures using Pandas, and loads the structured analytical tables into MotherDuck (**Silver Layer**).

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
 [ upload_to_s3 ]              [ load_to_motherduck ]
   (AWS S3 Data Lake)            (MotherDuck Cloud DWH)
   Bronze Layer                  Silver Layer
   raw/earthquakes_*.json        earthquakes_dw.silver_earthquakes
```

---

## — Tech Stack & Software Practices

- **Workflow Orchestration:** Apache Airflow 2.x (TaskFlow API `@dag`, `@task`).
- **Cloud Storage (Data Lake):** AWS S3 (Bronze Layer auditing).
- **Data Warehouse:** MotherDuck & DuckDB (Silver Layer querying).
- **Data Transformation:** Python 3.12 & Pandas (JSON flattening & epoch timestamp conversion).
- **Containerization:** Astro CLI / Docker Compose.
- **Security & Governance:** Airflow Connections (`aws_default`) & Airflow Variables (`motherduck_token`) to prevent credential hardcoding.

---

## — Database Schema (Silver Layer: `silver_earthquakes`)

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
