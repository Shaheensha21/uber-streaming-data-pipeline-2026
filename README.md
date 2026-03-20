# 🚖 Uber End-to-End Streaming Data Engineering Pipeline

## 📌 Project Overview

This project demonstrates a real-time data engineering pipeline that simulates Uber ride events, streams them through Azure Event Hub, processes them using PySpark Structured Streaming, and stores the results in Delta Lake on Azure Data Lake.

The pipeline follows modern data engineering architecture with Bronze, Silver, and Gold layers and implements a Star Schema for analytics.

---

## 🏗️ Architecture

**Flow:**

Producer → Azure Event Hub → Databricks (PySpark Streaming) → Delta Lake → Analytics

---

## ⚙️ Tech Stack

* Python
* PySpark (Structured Streaming)
* Azure Event Hub
* Azure Data Lake Storage Gen2
* Delta Lake
* Databricks

---

## 🔄 Data Pipeline Steps

### 1️⃣ Data Generation

* Simulated Uber ride events using Python producer
* Fields include:

  * ride_id
  * driver_id
  * location
  * fare
  * timestamp

---

### 2️⃣ Event Streaming

* Events pushed into Azure Event Hub
* Acts as real-time ingestion layer

---

### 3️⃣ Data Ingestion

* Databricks reads streaming data from Event Hub
* Uses encrypted connection string

---

### 4️⃣ Stream Processing

* Data parsed using schema
* Transformations applied:

  * JSON parsing
  * Type casting
  * Filtering
  * Aggregations
  * Window operations (rides per minute)

---

### 5️⃣ Storage Layer

* Data stored in Azure Data Lake using Delta format

**Layers:**

* Bronze → Raw streaming data
* Silver → Cleaned data
* Gold → Aggregated data

---

### 6️⃣ Data Modeling (Star Schema)

#### Fact Table

* fact_rides

  * ride_id
  * driver_id
  * location
  * event_time
  * fare

#### Dimension Tables

* dim_driver
* dim_location
* dim_time

---

## 📊 Sample Analytics

* Average fare per driver
* Rides per minute (window function)
* Revenue insights

---

## 🚀 How to Run

1. Start the producer:

```bash
python producer/producer.py
```

2. Run Databricks notebooks in order:

* 01 → 05

3. Monitor streaming queries

4. Stop streams when needed

---

## 💡 Key Learnings

* Real-time streaming architecture
* Event-driven data pipelines
* Delta Lake usage
* Azure cloud integration
* Handling streaming vs batch data

---

## 🏆 Outcome

Successfully built a scalable, real-time data pipeline using modern data engineering tools and cloud services.

---

## 📌 Future Improvements

* Add Power BI dashboard
* Deploy using CI/CD
* Add Kafka integration
* Implement real-time alerts

---

## 👨‍💻 Author

Shaik Abdul Shahansha
