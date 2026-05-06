# **UBER REAL-TIME DATA ENGINEERING PROJECT**

#### **Watch The Full Project On YouTube** - https://youtu.be/5KIbhHo6GJA?si=ktBADBZbM3IqRJ2s

A complete, production-grade end-to-end streaming data engineering platform built on Azure Databricks — ingesting real-time Uber ride events, processing them through a Medallion architecture, and delivering a fully modeled star schema Gold layer powered by Spark Declarative Pipelines (Delta Live Tables).

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                            │
│                                                                                 │
│   Azure Event Hubs          Bulk Historical Load                                │
│   (Real-Time Stream)   +    (Azure Data Factory)                                │
└──────────────┬──────────────────────┬──────────────────────────────────────────┘
               │                      │
               ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER — Raw Ingestion                                  │
│                                                                                 │
│   rides_raw  ──────────────────────────────────────────────────────────────►   │
│   (Streaming Table · Output: 5 records · 4s)                                   │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    STAGING LAYER — Unified Stream                                │
│                                                                                 │
│   stg_rides  ──────────────────────────────────────────────────────────────►   │
│   (Streaming Table · DLT Append Flow · Output: 2K records · 6s)                │
│   Merges bulk historical + real-time stream into one clean landing zone        │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER — Cleaned & Transformed                          │
│                                                                                 │
│   silver_obt ──────────────────────────────────────────────────────────────►   │
│   (Streaming Table · PySpark OBT · Output: 2K records · 3s)                   │
│   Type casting · Deduplication · Validation · One Big Table                    │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
                    ▼                  ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER — Star Schema (6 Dimensions + Fact)              │
│                                                                                │
│  dim_booking_view  ──► dim_booking    (Upserted: 2K · 46s)                   │
│  dim_driver_view   ──► dim_driver     (Upserted: 2K · 54s)                   │
│  dim_passenger_view──► dim_passenger  (Upserted: 2K · 55s)                   │
│  dim_payment_view  ──► dim_payment    (Upserted: 4  · 54s)                   │
│  dim_vehicle_view  ──► dim_vehicle    (Upserted: 2K · 57s)                   │
│  dim_location_view ──► dim_location   (Upserted: 10 · 34s)                   │
│  fact_view         ──► fact           (Upserted: 2K · 9s)                    │
│                                                                                │
│  All tables: ✅ Green · Live · ACID-compliant Delta tables                    │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| **Cloud Platform** | Microsoft Azure |
| **Data Processing** | Azure Databricks, Apache Spark, PySpark |
| **Real-Time Ingestion** | Azure Event Hubs, Spark Structured Streaming |
| **Orchestration** | Azure Data Factory, Databricks Workflows |
| **Pipeline Framework** | Spark Declarative Pipelines (Delta Live Tables) |
| **Storage Format** | Delta Lake (ACID, Time Travel, Schema Evolution) |
| **Transformation** | dbt, Jinja macros, Snowflake |
| **Data Modeling** | Medallion Architecture, Star Schema, SCD Type-1/2 |
| **Governance** | Unity Catalog, RBAC, Data Lineage |
| **Version Control** | Git, GitHub |
| **Language** | Python, PySpark, SQL, Jinja |

---

## 📁 Project Structure

```
Uber_Data_Engineer_Streaming_Project/
│
├── Code_Files/
│   ├── bronze/
│   │   └── rides_raw.py              # Raw ingestion from Event Hubs
│   │
│   ├── staging/
│   │   └── stg_rides.py              # DLT append flow — bulk + stream unified
│   │
│   ├── silver/
│   │   └── silver_obt.py             # PySpark transformations — One Big Table
│   │
│   ├── gold/
│   │   ├── dim_booking.py            # Booking dimension
│   │   ├── dim_driver.py             # Driver dimension
│   │   ├── dim_passenger.py          # Passenger dimension
│   │   ├── dim_payment.py            # Payment method dimension
│   │   ├── dim_vehicle.py            # Vehicle dimension
│   │   ├── dim_location.py           # Location dimension
│   │   └── fact.py                   # Fact table
│   │
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/              # dbt staging models
│   │   │   ├── intermediate/         # dbt intermediate models
│   │   │   └── marts/                # dbt mart models (Snowflake)
│   │   ├── macros/                   # Jinja macros for reusable SQL
│   │   └── dbt_project.yml
│   │
│   ├── schemas/
│   │   └── rides_schema.py           # StructType schema definitions
│   │
│   ├── model.py                      # Data model definitions
│   └── connection.py                 # Azure connection configuration
│
├── Data/
│   └── sample_rides.json             # Sample ride event data
│
├── Docs/
│   └── architecture_diagram.png      # Pipeline DAG screenshot
│
├── .gitignore
└── README.md
```

## 🔄 Pipeline Deep Dive

### Bronze Layer — Raw Ingestion
Real-time Uber ride events are ingested from **Azure Event Hubs** using Spark Structured Streaming with exactly-once processing guarantees. Raw JSON payloads land in `rides_raw` as a Delta streaming table.

```python
import dlt

@dlt.table(
    comment="Raw ride events from Azure Event Hubs",
    table_properties={"quality": "bronze"}
)
def rides_raw():
    return (spark.readStream
        .format("eventhubs")
        .options(**ehConf)
        .load()
        .withColumn("rides", col("body").cast("string"))
    )
```

### Staging Layer — Unified Multi-Source Ingestion
The staging layer solves a classic production problem — merging **bulk historical data** and **real-time stream** into a single clean table using DLT append flows, without duplicating logic.

```python
# Empty streaming table as the target
dlt.create_streaming_table("stg_rides")

# Flow 1 — Historical/bulk load
@dlt.append_flow(target="stg_rides")
def rides_bulk():
    return spark.readStream.table("bulk_rides")

# Flow 2 — Real-time stream
@dlt.append_flow(target="stg_rides")
def rides_stream():
    return (dlt.read_stream("rides_raw")
        .withColumn("parsed", from_json(col("rides"), rides_schema))
        .select("parsed.*")
    )
```

### Silver Layer — Transformation & Cleansing
PySpark transformations applied to create a clean One Big Table (OBT):
- Schema casting (timestamps, numerics)
- Deduplication on `ride_id`
- Data quality validation
- Null handling and standardization

### Gold Layer — Star Schema
Six dimension tables and one fact table materialized as streaming Delta tables, updated via upsert (MERGE) on each pipeline run:

| Table | Type | Records | Description |
|---|---|---|---|
| `dim_booking` | Streaming | 2,000 | Booking details and status |
| `dim_driver` | Streaming | 2,000 | Driver profiles and ratings |
| `dim_passenger` | Streaming | 2,000 | Passenger information |
| `dim_payment` | Streaming | 4 | Payment method lookup |
| `dim_vehicle` | Streaming | 2,000 | Vehicle details |
| `dim_location` | Streaming | 10 | Pickup/dropoff city mapping |
| `fact` | Streaming | 2,000 | Ride transactions and metrics |

---

## 🧱 Data Model

```
                         ┌──────────────┐
                         │  dim_booking │
                         └──────┬───────┘
                                │
┌─────────────┐    ┌────────────▼──────────┐    ┌──────────────┐
│  dim_driver │────►                        ◄────│dim_passenger │
└─────────────┘    │         FACT           │    └──────────────┘
                   │    (ride_id FK → all   │
┌─────────────┐    │      dimensions)       │    ┌──────────────┐
│ dim_payment │────►                        ◄────│ dim_vehicle  │
└─────────────┘    └────────────┬──────────┘    └──────────────┘
                                │
                         ┌──────▼───────┐
                         │ dim_location │
                         └──────────────┘
```

**Fact table metrics:**
- `base_fare`, `distance_fare`, `time_fare`
- `surge_multiplier`, `subtotal`, `tip_amount`, `total_fare`
- `distance_miles`, `duration_minutes`
- `driver_rating`, `passenger_rating`

---

## ⚙️ Key Design Decisions

**1. DLT Append Flow for dual-source ingestion**
Instead of building two separate pipelines, a single `stg_rides` streaming table accepts both bulk historical loads and real-time Event Hubs streams via `@dlt.append_flow`. This keeps the downstream logic unified and eliminates duplication.

**2. SCD Type 1 for location dimensions**
City and location data changes infrequently and history isn't required for analytics — SCD Type 1 (overwrite with `updated_at`) was the right call. SCD Type 2 would add complexity without analytical value.

**3. One Big Table (OBT) at Silver**
The silver layer materializes a denormalized OBT before splitting into star schema at Gold. This makes transformations explicit and testable, and gives a clean single source for all Gold layer fans.

**4. Upsert pattern at Gold**
All Gold dimension tables use MERGE (upsert) rather than append — ensuring idempotency and safe reruns without creating duplicates.

---

## 🚀 Getting Started

### Prerequisites
- Azure subscription with Databricks, Event Hubs, and Data Factory provisioned
- Databricks Unity Catalog enabled
- Python 3.9+
- dbt Core with Snowflake adapter

### Setup

```bash
# Clone the repo
git clone https://github.com/AshithaUppalapati/Uber_Data_Engineer_Streaming_Project.git
cd Uber_Data_Engineer_Streaming_Project

# Configure connection
cp Code_Files/connection.py.example Code_Files/connection.py
# Add your Azure credentials (never commit real credentials)
```

### Running the Pipeline

```bash
# 1. Deploy DLT pipeline via Databricks UI
#    Workflows → Delta Live Tables → Create Pipeline
#    Point to: Code_Files/bronze/ + staging/ + silver/ + gold/

# 2. Trigger ADF pipeline for bulk load
#    Azure Data Factory → Pipelines → Run

# 3. Start Event Hubs stream
#    Configure producer to send ride events to your Event Hub
```

### dbt Setup (Snowflake transformation layer)

```bash
cd Code_Files/dbt
pip install dbt-snowflake
dbt deps
dbt run
dbt test
```

---

## 📊 Pipeline Performance

| Stage | Processing Time | Output Records |
|---|---|---|
| `rides_raw` (Bronze) | 4s | 5 |
| `stg_rides` (Staging) | 6s | 2,000 |
| `silver_obt` (Silver) | 3s | 2,000 |
| `dim_*` (Gold Dims) | 46–57s | 2,000 each |
| `fact` (Gold Fact) | 9s | 2,000 |
| `dim_location` | 34s | 10 |

---

## 🔐 Data Governance

- **Unity Catalog** — centralized metadata, lineage tracking, and access control
- **RBAC** — role-based access enforced at catalog, schema, and table level
- **Data masking** — PII fields (passenger email, phone, driver license) masked for non-privileged roles
- **Audit logging** — all table access tracked via Unity Catalog audit logs
- **Schema enforcement** — Delta Lake enforces schema on write — bad records rejected, not silently corrupted

---

## 📚 What I Learned

**Streaming + batch unification** — The DLT append flow pattern for merging historical and real-time sources into one table is the cleanest solution I've worked with. Before this, I'd have built two pipelines and a reconciliation job.

**SCD design is a business question first** — Choosing Type 1 vs Type 2 isn't a technical decision. It's: "Will anyone ever need to know what this looked like before?" Getting that wrong rewrites history.

**Teaching accelerates learning** — Explaining the Medallion pattern to junior engineers while building this project exposed gaps in my own understanding faster than any documentation.

**Git discipline matters** — Daily commits with meaningful messages create a story of how a pipeline was built. One-shot uploads don't.

---

## 📄 License

This project is built for learning and portfolio purposes, based on the [Azure Databricks Streaming Project tutorial](https://www.youtube.com/watch?v=5KIbhHo6GJA).

![Project Architecture](https://github.com/anshlambagit/Uber_Data_Engineer_Project/blob/main/architecture.png)



