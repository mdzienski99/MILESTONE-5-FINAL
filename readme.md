# 🎬 Movie Recommender System — Final Project

##  Michal Dzienski & Rayan Rabbi

---

##  Live API

###  Base URL

https://milestone-4-api.onrender.com

###  Health Check

https://milestone-4-api.onrender.com/healthz

###  Example

https://milestone-4-api.onrender.com/recommend/1?k=5&model=auto

### A/B Testing Examples
https://milestone-4-api.onrender.com/recommend/2?k=5&model=auto  
https://milestone-4-api.onrender.com/recommend/3?k=5&model=auto  

### Model Switching

https://milestone-4-api.onrender.com/switch?version=v20260413235853  
https://milestone-4-api.onrender.com/switch?version=v20260414000022  

---

##  Deliverables

*  Report: [report.pdf](./report.pdf)
*  Slides: [slides.pdf](./slides.pdf)
*  Video: https://www.youtube.com/watch?v=QrzhACsgEj4 

---

##  Key Features

* A/B Testing (popularity vs item-based model)
* Model versioning + registry
* Hot model switching (`/switch`)
* Monitoring (latency, availability)
* Kafka-based streaming simulation
* Automated retraining (GitHub Actions)

---

##  Example Output

```json
{
  "user_id": 1,
  "model_served": "item_cf",
  "ab_group": "B",
  "recommendations": [90, 300]
}
```

---

#  Quick Start (Local Setup)

## 1. Setup

```bash
git clone <repo-url>
cd Milestone-Project-COT-6930
python -m venv .venv
pip install -r requirements.txt
```

---

## 2. Run API

```bash
uvicorn service.app:app --reload --port 8000
```

Endpoints:

* `/healthz`
* `/recommend/{user_id}`
* `/metrics`

---

## 3. Kafka (local)

```bash
docker compose up -d
powershell -File scripts/kafka_start_and_init.ps1
kcat -b localhost:9092 -L
```

---

## 4. Start Pipeline

```bash
python stream/kafka_ingest.py
```

---

## 5. Test API

```bash
curl "http://localhost:8000/recommend/1001?k=10&model=popularity"
```

---

#  Deployment (Cloud Run - optional)

## Build & Push

```bash
docker build -t gcr.io/<GCP_PROJECT_ID>/recommender-api:latest .
docker push gcr.io/<GCP_PROJECT_ID>/recommender-api:latest
```

## Deploy

```bash
gcloud run deploy recommender-api \
  --image gcr.io/<GCP_PROJECT_ID>/recommender-api:latest \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092,TEAM_NAME=team"
```

---

#  Additional Scripts

## Model Comparison

```bash
python stream/model_comparison.py
```

## Probe Pipeline

```bash
python scripts/probe.py --num-users 50 --calls-per-user 2 --json
```

---

#  Notes

* Kafka runs locally (simulation)
* API deployed on Render
* First request may be slow due to cold start

---

#  Summary

This project demonstrates:

* full ML pipeline
* real-time API
* A/B testing
* monitoring + traceability
* production-style system design
