# On-Prem Data Lakehouse using Docker

An end-to-end local Data Lakehouse implementation leveraging MinIO for S3-compatible object storage, DuckDB for high-performance processing, and Postgres as the serving layer. This project demonstrates the Medallion Architecture (Bronze, Silver, Gold) entirely within a containerized environment.

---

## 🚀 Overview

The goal of this project is to simulate a modern enterprise data platform on a single machine. It covers the full lifecycle of data: from raw ingestion to business-ready analytics.

---

## Tech Stack

- **Orchestration:** Docker & Docker Compose
- **Object Storage:** MinIO (S3-compatible)
- **Processing Engine:** DuckDB / Python
- **Serving Layer:** PostgreSQL
- **Language:** Python (Pandas / PyArrow)

---

## 🏗️ Architecture & Phases

### Phase 1: Infrastructure Setup

The foundation is built on a custom Docker network allowing seamless communication between storage and compute.

- **MinIO:** Configured with specific buckets for the Medallion layers
- **Postgres:** Set up as the final destination for curated insights
- **Networking:** Static container naming for easy internal routing

---

### Phase 2: Bronze Layer (Ingestion)

- Generation of realistic synthetic transaction datasets
- Python-based ingestion scripts that push raw CSV/JSON files into `s3://bronze/`
- Verification of data integrity in object storage

---

### Phase 3: Silver Layer (Transformation)

- **Schema Enforcement:** Ensuring data types are consistent
- **Data Cleaning:** Handling null values and deduplication
- **Optimization:** Converting raw files into Parquet format for faster analytical performance and implementing partitioning strategies

---

### Phase 4: Gold Layer (Curation)

- Aggregating Silver data into business-level entities (e.g., daily revenue, customer lifetime value)
- Creating "Source of Truth" tables optimized for BI tools

---

### Phase 5: Analytics & Serving

- Utilizing DuckDB to query the Gold Parquet files directly using SQL
- Loading final curated datasets into PostgreSQL for low-latency application access or dashboarding
