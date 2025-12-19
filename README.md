# On-Prem Data Lakehouse using Docker

A production-style, cloud-agnostic data lakehouse built with open-source tools, simulating how banks and regulated organizations implement analytics platforms on-premises.

## 🎯 Project Overview

This project demonstrates a complete data lakehouse implementation using:
- **Medallion Architecture** (Bronze → Silver → Gold)
- **Separation of storage and compute**
- **Containerized infrastructure**
- **Analytics-ready serving layer**

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA LAKEHOUSE                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Raw Data (CSV)                                             │
│       ↓                                                     │
│  ┌──────────────────────────────────────────────┐          │
│  │         MinIO (S3-Compatible Storage)        │          │
│  ├──────────────────────────────────────────────┤          │
│  │  Bronze Layer → Silver Layer → Gold Layer    │          │
│  │  (Raw Data)     (Cleaned)      (Curated)     │          │
│  └──────────────────────────────────────────────┘          │
│       ↓                    ↓                                │
│  DuckDB (Analytics)   PostgreSQL (Serving)                  │
│       ↓                    ↓                                │
│  Ad-hoc Queries       BI Tools / Apps                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 🛠️ Tech Stack

- **Docker & Docker Compose** - Infrastructure as code
- **MinIO** - S3-compatible object storage (Data Lake)
- **Python + Pandas** - ETL pipelines
- **DuckDB** - OLAP query engine for lakehouse
- **PostgreSQL** - Serving/warehouse layer
- **SQL** - Analytics queries

## 📁 Project Structure
```
onprem-lakehouse/
├── docker-compose.yml          # Infrastructure definition
├── .env                        # Environment configuration
├── docker/
│   └── postgres/
│       └── init.sql           # PostgreSQL initialization
├── scripts/
│   ├── generate_data.py       # Generate sample data
│   ├── ingest_bronze.py       # Bronze layer ingestion
│   ├── transform_silver.py    # Silver layer transformation
│   ├── curate_gold.py         # Gold layer curation
│   ├── analytics_duckdb.py    # DuckDB analytics
│   ├── load_postgres.py       # Load to PostgreSQL
│   └── run_pipeline.py        # Master orchestrator
├── data/
│   └── raw/                   # Raw CSV files
├── sql/
│   └── analytics.sql          # Sample analytics queries
└── README.md
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- 8GB RAM minimum
- 10GB free disk space

### 1. Clone and Setup
```bash
git clone <your-repo>
cd onprem-lakehouse
```

### 2. Start Infrastructure
```bash
docker-compose up -d
```

Wait for all containers to start (~30 seconds).

### 3. Install Python Dependencies
```bash
docker-compose exec etl-runner pip install minio psycopg2-binary pandas duckdb python-dotenv
```

### 4. Run Complete Pipeline
```bash
docker-compose exec etl-runner python scripts/run_pipeline.py
```

This executes the entire pipeline:
- Generates 10,000 transactions
- Ingests to Bronze layer
- Transforms to Silver layer
- Curates Gold layer
- Runs DuckDB analytics
- Loads to PostgreSQL

## 📊 Data Flow

### Bronze Layer (Raw)
- **Location:** MinIO bucket `bronze/`
- **Format:** Raw CSV files
- **Partitioning:** `transactions/YYYY-MM-DD/`
- **Characteristics:** Append-only, immutable, no transformations

### Silver Layer (Cleaned)
- **Location:** MinIO bucket `silver/`
- **Format:** Cleaned CSV files
- **Transformations:**
  - Null handling
  - Schema enforcement
  - Data type casting
  - Derived columns (date parts)
  - Standardization

### Gold Layer (Curated)
- **Location:** MinIO bucket `gold/`
- **Format:** Aggregated CSV files
- **Datasets:**
  - `daily_summary` - Daily transaction metrics
  - `city_summary` - City-level performance
  - `transaction_type_summary` - Transaction type breakdown
  - `hourly_pattern` - Hourly transaction patterns

### Serving Layer (PostgreSQL)
- **Location:** PostgreSQL database
- **Schema:** `gold`
- **Purpose:** BI tools, applications, SQL queries

## 🔍 Access Points

### MinIO Console
```
URL: http://localhost:9001
Username: admin
Password: admin123
```

### PostgreSQL
```
Host: localhost
Port: 5432
Database: analytics
Username: lakehouse
Password: lakehouse123
```

**Connect via psql:**
```bash
docker-compose exec postgres psql -U lakehouse -d analytics
```

### Sample Queries
```bash
docker exec lakehouse-postgres psql -U lakehouse -d analytics -f /tmp/analytics.sql
```

## 📈 Sample Analytics

### Top Cities by Revenue
```sql
SELECT city, ROUND(total_amount::numeric, 2) as revenue
FROM gold.city_summary
ORDER BY total_amount DESC
LIMIT 5;
```

### Daily Transaction Volume
```sql
SELECT transaction_date, total_transactions
FROM gold.daily_summary
ORDER BY total_transactions DESC;
```

### Peak Transaction Hours
```sql
SELECT hour, total_transactions
FROM gold.hourly_pattern
ORDER BY total_transactions DESC;
```

## 🎓 Key Concepts Demonstrated

### Medallion Architecture
- **Bronze:** Raw data preservation
- **Silver:** Cleaned and standardized
- **Gold:** Business-ready aggregations

### Data Lakehouse Pattern
- Storage (MinIO) separated from compute (Python/DuckDB)
- Schema-on-read flexibility
- ACID transactions via Delta/Iceberg pattern (simulated)

### Production Best Practices
- Idempotent pipelines
- Date-based partitioning
- Auto-detection of latest data
- Error handling
- Metadata tracking

## 🔧 Individual Pipeline Steps

Run individual stages for testing:
```bash
# Generate data
docker-compose exec etl-runner python scripts/generate_data.py

# Bronze ingestion
docker-compose exec etl-runner python scripts/ingest_bronze.py

# Silver transformation
docker-compose exec etl-runner python scripts/transform_silver.py

# Gold curation
docker-compose exec etl-runner python scripts/curate_gold.py

# DuckDB analytics
docker-compose exec etl-runner python scripts/analytics_duckdb.py

# Load to PostgreSQL
docker-compose exec etl-runner python scripts/load_postgres.py
```

## 🛑 Stopping the Environment
```bash
docker-compose down
```

To remove all data (reset):
```bash
docker-compose down -v
```

## 📝 Dataset Schema

### Raw Transactions
- `transaction_id` - Unique transaction identifier
- `account_id` - Account identifier
- `amount` - Transaction amount
- `transaction_type` - deposit/withdrawal/transfer/payment
- `timestamp` - Transaction timestamp
- `city` - Transaction location

## 🚀 Future Enhancements

- [ ] Incremental data ingestion
- [ ] Apache Iceberg/Delta Lake integration
- [ ] Airflow orchestration
- [ ] Data quality monitoring
- [ ] Real-time streaming with Kafka
- [ ] dbt for transformations
- [ ] Grafana dashboards

## 📚 Learning Resources

- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Data Lakehouse Concept](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [DuckDB Documentation](https://duckdb.org/docs/)

## 🤝 Contributing

This is a learning project. Feel free to fork and extend!

## 📄 License

MIT License - free to use for learning and portfolio purposes.

---

**Built with ❤️ to demonstrate production-grade data engineering practices**