# 🎧 Guess My Mood  
**Predicting Daily Mood Using Music Listening Behavior and Weather Data**

---

## Project Overview
**Guess My Mood** is a data engineering and analytics project that combines **live music streaming data** and **historical weather data** to infer and visualize a user’s daily mood.  
By analyzing *yesterday’s listening behavior* alongside environmental conditions, the system produces behavioral insights and dashboard-ready metrics that reflect emotional and activity patterns.

This project demonstrates:
- Real-time and batch data integration  
- Cloud-native ELT pipelines  
- Distributed processing with Apache Spark  
- Analytical modeling

---

## 🏗️ Architecture Overview

```
Apple Music / Last.fm (Live API) and Open Meteo Historical API
              │
              ▼
        Python Ingestion
              │
              ▼
      Google Cloud Storage
              │
              ▼
        Dataproc (Spark)
              │
              ▼
        BigQuery (Analytics)
              │
              ▼
      dbt Transformations
              │
              ▼
     Dashboard ready tables
```

---

## 📊 Data Sources

### 1. Music Streaming Data (Live)
- **Source:** Apple Music history via Last.fm API  
- **Granularity:** Track-level listening events  
- **Key Fields:**
  - Song Name  
  - Album Name  
  - Play Duration (milliseconds)  
  - Event Timestamp  

### 2. Weather Data (Historical)
- **Source:** Public Weather API  
- **Granularity:** Hourly & daily  
- **Key Fields:**
  - Temperature  
  - Weather Condition  
  - Humidity  
  - Wind Speed  
  - Timestamp  


### 3. Apple Music Streaming History Data

### 4. Music Track metadata (Kaggle)
---

## 🧱 Tech Stack

| Layer | Tools |
|-----|------|
| Ingestion | Python, Requests |
| Storage | Google Cloud Storage |
| Processing | Apache Spark (Dataproc) |
| Orchestration | Dataproc Workflow Templates |
| Scheduling | Cloud Scheduler |
| Analytics | BigQuery |
| Cloud | Google Cloud Platform |

---

## ⚙️ Setup Instructions

### Prerequisites
- Google Cloud Project with billing enabled  
- Python 3.9+  
- `gcloud` CLI configured  
- Enabled APIs:
  - Dataproc  
  - BigQuery  
  - Cloud Storage  
  - Cloud Scheduler  

---

### 1️. Clone Repository
```bash
git clone https://github.com/your-username/guess-my-mood.git
cd guess-my-mood
```

---

### 2️. Python Environment
```bash
python -m venv mood
source mood/bin/activate   # Windows: mood\Scripts\activate
pip install -r requirements.txt
```

---

### 3️. Configure Secrets
Create a `.env` file:
```env
LASTFM_API_KEY=your_api_key
PROJECT_ID=your_gcp_project
BUCKET_NAME=your_gcs_bucket
```

### 4️. Create Dataproc Cluster
```bash
gcloud dataproc clusters create mood-cluster \
  --region=us-central1 \
  --single-node \
  --master-machine-type=n1-standard-2 \
  --image-version=2.3-debian12
```

### 5. Create CloudNAT router
```bash
gcloud compute routers create mood-router --network=default --region=us-central1
```

### 6. Create CloudNAT and attach to the router
```bash
gcloud compute routers nats create mood-nat \
--router=mood-router --region=us-central1 \
--auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
```

### 7. Create Dataproc Workflow Template
```bash
gcloud dataproc workflow-templates create mood-pipeline-template --region=us-central1
```

### 8. Add your Job to the Workflow Template
```bash
gcloud dataproc workflow-templates add-job pyspark --workflow-template=mood-pipeline-template --region=us-central1 --step-id=mood_step \
--jars=gs://BUCKET-PATH/jars/gcs-connector-hadoop3-2.2.9-shaded.jar,gs://BUCKET-PATH/jars/guice-5.1.0.jar,gs://BUCKET-PATH/jars/javax.inject-1.jar,gs://BUCKET-PATH/jars/spark-3.5-bigquery-0.43.1.jar --py-files=gs://BUCKET-PATH/pipeline.zip gs://BUCKET-PATH/main.py
```

### 9. Add your cluster to the Workflow Template
```bash
gcloud dataproc workflow-templates set-cluster-selector mood-pipeline-template --region=us-central1 --cluster-labels=goog-dataproc-cluster-name=mood-cluster
```

### 10. Create Pub/Sub topic
```bash
gcloud pubsub topics create mood-trigger
```

### 11. Create Cloud Scheduler Job
```bash
gcloud scheduler jobs create pubsub mood-job --schedule="0 2 * * *" --time-zone="Europe/Berlin" --topic="mood-trigger" --message-body="start" --location=us-central1
```

### 12. Deploy Cloud Function
- Navigate to the root of the repo
```bash
gcloud functions deploy trigger-dataproc --runtime=python310 --trigger-topic="mood-trigger" \
--entry-point=trigger_dataproc --region=us-central1 --timeout=300s --memory=256MB --source=. --no-gen2
```

---

## Data Processing Logic (Daily at 2:00am)

1. Fetch **previous day’s listening history**  
2. Normalize timestamps & durations  
3. Aggregate listening metrics by hour  
4. Join weather data on timestamp  
5. Compute:
   - Listening intensity  
   - Cumulative songs played  
   - Average audio features  
   - Behavioral deviation from baseline  
6. Store curated tables in **BigQuery**  
