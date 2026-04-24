# MSBA-305-Data-Processing-Project

## NYC Data Analytics — Microsoft Fabric Medallion Architecture
##  Overview

This is a **three-layer medallion architecture** (Bronze → Silver → Gold) built on **Microsoft Fabric** that ingests, cleans, and analyzes NYC data across three domains:

- **Weather** — Hourly weather data from Open-Meteo Archive API
- **Traffic** — NYC traffic volume counts from NYC Open Data API
- **311 Service Requests** — NYC complaint and service request data from Socrata Open Data API
  
This project aims to build a framework that connects citizen complaints with weather, traffic, and agency performance to identify the drivers of complaint volume and resolution capacity, enabling cities worldwide to make data‑driven staffing and operational decisions.

---

##  Architecture Overview

### Medallion Layers

```
┌─────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                           │
│              (Unified Analytical Table)                     │
│    - Fact table: 1 row = 1 borough × 1 hour                 │
│    - Joins all three Silver tables on borough + timestamp   │
│    - Ready for analysis and visualization                   │
└─────────────────────────────────────────────────────────────┘
                              ↑
         ┌────────────────────┼────────────────────┐
         ↑                    ↑                    ↑
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │   SILVER    │   │   SILVER    │   │   SILVER    │
    │   WEATHER   │   │   TRAFFIC   │   │     311     │
    │  (16 steps) │   │  (9 steps)  │   │  (16 steps) │
    └─────────────┘   └─────────────┘   └─────────────┘
         ↑                    ↑                    ↑
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │   BRONZE    │   │   BRONZE    │   │   BRONZE    │
    │   WEATHER   │   │   TRAFFIC   │   │     311     │
    │(API Fetch)  │   │(API Fetch)  │   │(API Fetch)  │
    └─────────────┘   └─────────────┘   └─────────────┘
         ↑                    ↑                    ↑
    Open-Meteo        NYC Open Data          Socrata
      Archive            API                  API
```

---

##  Project Structure

### Bronze Layer (Raw Ingestion)

| Notebook | Purpose | Data Source | Granularity |
|----------|---------|-------------|------------ |
| `Bronze_Weather_Ingestion.ipynb` | Fetch hourly weather for NYC boroughs | Open-Meteo Archive API | Hourly per borough |
| `Bronze_TrafficControl_Ingestion.ipynb` | Fetch NYC traffic volume counts | NYC Open Data API (7ym2-wayt) | 15-min intervals per location |
| `Bronze_311_ingestion.ipynb` | Fetch NYC 311 service requests | Socrata Open Data API | Individual complaint records |

**Bronze Layer Features:**
-  API-first design with retry logic (exponential backoff, max 3 retries)
-  Schema validation and type enforcement
-  Deduplication before writes
-  Provenance tracking: `ingest_run_id`, `ingest_timestamp`, `ingestion_date`
-  Quarantine handling for invalid records (separate quarantine tables)
-  Idempotent writes using Delta Lake with append mode
-  Batch-only (no streaming)

### Silver Layer (Cleaned & Validated)

| Notebook | Purpose | Input | Cleaning Steps | Output |
|----------|---------|-------|-----------------|--------|
| `05_silver_weather.ipynb` | Clean weather data; detect outliers | `bronze_weather_nyc` | 9 steps | `silver_stg_weather` + `quarantine_weather` |
| `06_silver_traffic.ipynb` | Clean traffic; aggregate 15-min → hourly | `bronze_traffic` | 9 steps | `silver_stg_traffic` + `quarantine_traffic` |
| `Silver_311_v1.ipynb` | Clean 311 records; SCD Type-2 tracking | `bronze_311` | 16 steps | `silver_stg_nyc_311` + `silver_quarantine_311` |

**Silver Layer Features:**
-  Null rejection and type casting
-  Deduplication (row-level and source-level)
-  Data validation (date ranges, coordinate bounds, business rules)
-  Outlier detection and quarantine (suspicious but possibly legitimate rows)
-  Standardization (text normalization, column pruning, code removal)
-  SCD Type-2 change tracking (311 data only)
-  Multi-strategy recovery (e.g., borough/district from coordinates)
-  Aggregation where appropriate (traffic 15-min → hourly)

**Cleaning Step Examples:**

**Weather (05_silver_weather.ipynb):**
1. Null rejection for critical fields
2. Type casting (temperature, precipitation, snowfall to numeric)
3. Coordinate validation (within NYC bounds)
4. Date validation (timestamp in reasonable range)
5. Range checks (temperature -40°C to 50°C, humidity 0-100%, precipitation ≥ 0)
6. Outlier detection (extreme weather flagged to quarantine)
7. Unit conversion (snowfall ÷ 7)
8. Column pruning
9. Provenance addition (cleaning_run_id, clean_timestamp)

**Traffic (06_silver_traffic.ipynb):**
1. Null rejection
2. Type casting (count to integer, coordinates to numeric)
3. Coordinate validation
4. Borough name standardization (title case)
5. Volume range validation (0 ≤ count ≤ reasonable max)
6. Aggregation: 15-min intervals → hourly per borough
7. Deduplication after aggregation
8. Column pruning
9. Provenance addition

**311 (Silver_311_v1.ipynb):**
1. Null rejection (complaint_type, created_date required)
2. Source deduplication (unique_id, created_date)
3. Type casting (coordinates, dates)
4. Column pruning (keep only analytical columns)
5. Status harmonization (standardize complaint statuses)
6. Routing code removal
7. Text standardization (uppercase, trim)
8. Business rule validation (created ≤ closed)
9. Coordinate validation
10. Borough recovery (from coordinates if missing)
11. Park facility name recovery
12. Council district recovery
13. SCD Type-2: track version, valid_from, valid_to
14. Provenance addition
15. Route unpair-able records to quarantine

### Gold Layer (Analytical)

| Notebook | Purpose | Input Tables | Grain | Output |
|----------|---------|--------------|-------|--------|
| `Gold_00_silver_schema_explorer.ipynb` | Inspect Silver schemas before joining | All Silver tables | N/A | Display summaries & samples |
| `Gold_v1.ipynb` | Join all three Silver tables | `silver_stg_weather`, `silver_stg_traffic`, `silver_stg_nyc_311` | 1 row = borough × hour | `gold_nyc_analytics` |

**Gold Layer Features:**
-  **Fact table design:** 1 row = 1 borough × 1 hour
-  **Three-way join:** Weather (hourly) + Traffic (hourly) + 311 (aggregated to hour)
-  **Join keys:** `borough` + `timestamp` (hour-truncated)
-  **SCD handling:** Includes most-recent version of each 311 complaint (Type-2)
-  **Aggregated metrics:** Count of complaints per hour, sum of traffic volumes
-  **Query-ready:** All dimensions and measures pre-computed

**Gold Schema (Conceptual):**
```
gold_nyc_analytics (borough_hour_fact):
├── borough (string)                    ← Dimension
├── timestamp (timestamp)               ← Dimension (hour)
├── weather_temp_c (double)
├── weather_humidity_pct (double)
├── weather_precipitation_mm (double)
├── weather_snowfall_mm (double)
├── traffic_volume_count (integer)
├── complaints_311_count (integer)
├── complaints_311_sample (array)       ← Top complaint types
└── ...
```

---

##  Quick Start

### Prerequisites

- **Microsoft Fabric** workspace with Lakehouse provisioned
- **Spark 3.x** (Fabric-managed)
- **Python 3.x**
- Libraries: `pyspark`, `requests`, `delta`, `shapely`, `geopandas`, `pyproj`

### Step 1: Set Up Lakehouse

All notebooks assume a **Lakehouse** with the following folder structure:

```
lakehouse/
└── Project Data/
    ├── bronze/
    │   ├── weather/
    │   ├── traffic/
    │   └── 311/
    ├── silver/
    │   ├── weather/
    │   ├── traffic/
    │   └── 311/
    └── gold/
```

This is created automatically by the Bronze notebooks using Fabric's file system API.

### Step 2: Run Bronze Layer (Sequential)

Execute in this order:

```bash
1. Bronze_Weather_Ingestion.ipynb
2. Bronze_TrafficControl_Ingestion.ipynb
3. Bronze_311_ingestion.ipynb
```

**Expected output:**
- `bronze_weather_nyc` (Delta table)
- `bronze_traffic` (Delta table)
- `bronze_311` (Delta table)
- Plus quarantine tables for rejected records
- All raw files are saved under Files directory and all tables are saved under the Tables directory in Fabric, 

**Runtime:** ~2-5 minutes total (depending on API availability)

### Step 3: Run Silver Layer (Sequential)

```bash
1. 05_silver_weather.ipynb
2. 06_silver_traffic.ipynb
3. Silver_311_v1.ipynb
```

**Expected output:**
- `silver_stg_weather` (cleaned, validated)
- `silver_stg_traffic` (cleaned, aggregated)
- `silver_stg_nyc_311` (cleaned, SCD Type-2)
- Plus quarantine tables for rows that failed cleaning

### Step 4: Inspect Silver Schemas

```bash
Gold_00_silver_schema_explorer.ipynb
```

This is optional but recommended before moving to Gold. Displays:
- Column names, types, nullability
- Row counts
- 10-row samples from each Silver table

### Step 5: Run Gold Layer

```bash
Gold_v1.ipynb
```

or

```bash
Gold_v1__1_.ipynb
```

(Both are equivalent; kept for comparison/versioning.)

**Expected output:**
- `gold_nyc_analytics` (unified fact table, borough × hour grain)

---

##  Data Dictionary

### Bronze Tables

#### `bronze_weather_nyc`
| Column | Type | Null? | Notes |
|--------|------|-------|-------|
| `timestamp` | timestamp | No | UTC time |
| `borough` | string | No | Manhattan, Brooklyn, Queens, Bronx, Staten Island |
| `latitude` | double | No | Borough centroid |
| `longitude` | double | No | Borough centroid |
| `temp_celsius` | double | Yes | Temperature in °C |
| `humidity_pct` | double | Yes | 0-100% |
| `precipitation_mm` | double | Yes | ≥ 0 |
| `snowfall_mm` | double | Yes | ≥ 0 |
| `weather_code` | integer | Yes | WMO code |
| `ingest_run_id` | string | No | UUID of ingest run |
| `ingest_timestamp` | timestamp | No | When record was ingested |
| `ingestion_date` | date | No | Date of ingest |

#### `bronze_traffic`
| Column | Type | Null? | Notes |
|--------|------|-------|-------|
| `count_id` | string | No | Unique counter ID |
| `timestamp` | timestamp | No | 15-min interval |
| `borough` | string | Yes | NYC borough name |
| `volume` | integer | Yes | Vehicle count |
| `latitude` | double | Yes | Counter location |
| `longitude` | double | Yes | Counter location |
| `ingest_run_id` | string | No | UUID of ingest run |
| `ingest_timestamp` | timestamp | No | When record was ingested |
| `ingestion_date` | date | No | Date of ingest |

#### `bronze_311`
| Column | Type | Null? | Notes |
|--------|------|-------|-------|
| `unique_id` | string | No | Complaint ID |
| `created_date` | timestamp | No | When reported |
| `closed_date` | timestamp | Yes | When resolved |
| `complaint_type` | string | No | Category (Heat, Noise, etc.) |
| `status` | string | Yes | Open, Closed, In Progress |
| `borough` | string | Yes | NYC borough |
| `latitude` | double | Yes | Complaint location |
| `longitude` | double | Yes | Complaint location |
| `council_district` | integer | Yes | District number |
| `park_facility_name` | string | Yes | Park/facility (if applicable) |
| `ingest_run_id` | string | No | UUID of ingest run |
| `ingest_timestamp` | timestamp | No | When record was ingested |
| `ingestion_date` | date | No | Date of ingest |

### Silver Tables

#### `silver_stg_weather`
Same schema as `bronze_weather_nyc` plus:
- `cleaning_run_id` (string) — UUID of cleaning run
- `clean_timestamp` (timestamp) — When row was cleaned
- Validated and null-checked; extreme weather rows may be in `quarantine_weather`

#### `silver_stg_traffic`
Same as Bronze, but:
- Aggregated to **hourly** per borough (was 15-min intervals)
- `volume` is sum of 15-min counts within the hour
- Deduped and null-checked; suspicious rows in `quarantine_traffic`

#### `silver_stg_nyc_311`
Same core fields as Bronze plus **SCD Type-2 tracking:**
- `scd_version` (integer) — 1, 2, 3, ... if record updates
- `valid_from` (timestamp) — When this version became active
- `valid_to` (timestamp) — When replaced by next version (null = current)
- `is_current` (boolean) — True for active version
- All records with SCD Type-2 history preserved

### Gold Table

#### `gold_nyc_analytics`
| Column | Type | Notes |
|--------|------|-------|
| `borough` | string | Manhattan, Brooklyn, Queens, Bronx, Staten Island |
| `hour` | timestamp | UTC hour (hour-truncated) |
| `temp_celsius` | double | Weather temperature |
| `humidity_pct` | double | Weather humidity |
| `precipitation_mm` | double | Weather precipitation |
| `snowfall_mm` | double | Weather snowfall |
| `traffic_volume_count` | long | Sum of traffic counts in the hour |
| `complaints_311_count` | long | Count of complaints in the hour |
| `complaints_311_top_types` | array | Top 3 complaint types (most recent) |
| ... | ... | (Additional aggregates as defined in Gold_v1.ipynb) |

**Grain:** 1 row = 1 borough × 1 hour

**Fact table:** Enables analysis like:
- "In which hours did high temperature correlate with more 311 complaints?"
- "Do weather events affect traffic volume?"
- "Which borough has the most complaints per unit traffic volume?"

---

##  Configuration & Parameters

### API Endpoints

**Open-Meteo Archive API** (Weather)
- Endpoint: `https://archive-api.open-meteo.com/v1/archive`
- Parameters: `latitude`, `longitude`, `start_date`, `end_date`, `hourly`
- Rate limit: No hard limit (but respectful backoff used)
- Data: 1950–present

**NYC Open Data API** (Traffic)
- Endpoint: `https://data.cityofnewyork.us/api/views/{dataset_id}/rows.json`
- Dataset: `7ym2-wayt` (NYC Traffic Volume Counts)
- Rate limit: Configurable batching; code uses 50K records/batch
- Data: 2014–present

**Socrata API** (311 Complaints)
- Endpoint: `https://data.cityofnewyork.us/api/views/{dataset_id}/rows.json`
- Dataset: `a2nx-4u46` (NYC 311 Service Requests)
- Rate limit: Similar to Open Data API
- Data: 2003–present

### Key Parameters

**Borough Coordinates** (Weather)
```
Manhattan:    40.7128, -74.0060
Brooklyn:     40.6501, -73.9496
Queens:       40.7282, -73.7949
Bronx:        40.8448, -73.8648
Staten Island: 40.5801, -74.1502
```

**Data Validation Ranges**
- Temperature: -40°C to 50°C
- Humidity: 0–100%
- Precipitation: ≥ 0 mm
- Snowfall: ≥ 0 mm (divided by 7 to convert units)
- Traffic count: 0 to [reasonable max per location]

**Cleaning Triggers**
- Any critical null → quarantine
- Timestamp outside data range → quarantine
- Coordinates outside NYC bounds → quarantine
- Temperature, humidity, precipitation ranges violated → quarantine/flag
- Status not in allowed list → harmonize or quarantine

---

##  Technical Details

### Tech Stack

- **Compute:** Apache Spark 3.x (Fabric-managed)
- **Storage:** Delta Lake (Fabric Lakehouse)
- **Languages:** Python (PySpark)
- **APIs:** REST (Open-Meteo, NYC Open Data, Socrata)
- **Libraries:**
  - `pyspark` — SQL, DataFrames, Spark functions
  - `requests` — HTTP API calls
  - `delta` — DeltaTable operations
  - `shapely` — Geometric validation (coordinates)
  - `geopandas` — Geospatial operations
  - `pyproj` — Coordinate system transformations
  - `notebookutils` — Fabric notebook utilities

### Error Handling

**API Failures:**
- Retry logic: up to 3 attempts with exponential backoff
- Transient errors (429, 503) retried; permanent errors (404, 401) fail immediately

**Data Validation Failures:**
- Critical nulls → quarantine (never imputed)
- Invalid values → quarantine or harmonized (e.g., status codes)
- Coordinate out-of-bounds → recover from geometry or quarantine
- Type mismatches → cast or quarantine

**Deduplication:**
- Bronze: before write (ensures idempotent ingestion)
- Silver: implicit via cleaning steps; explicit for 311 source-level dupes

---

##  Example Queries (Gold Table)

Once you've populated `gold_nyc_analytics`, try these analytical queries:

### 1. Top complaint types by borough and temperature
```sql
SELECT 
  borough,
  CASE 
    WHEN temp_celsius < 0 THEN 'Freezing'
    WHEN temp_celsius BETWEEN 0 AND 10 THEN 'Cold'
    WHEN temp_celsius BETWEEN 10 AND 20 THEN 'Cool'
    WHEN temp_celsius BETWEEN 20 AND 30 THEN 'Warm'
    ELSE 'Hot'
  END AS temp_category,
  complaints_311_top_types[0] AS top_complaint,
  COUNT(*) AS hours,
  AVG(complaints_311_count) AS avg_complaints_per_hour
FROM gold_nyc_analytics
GROUP BY borough, temp_category, complaints_311_top_types[0]
ORDER BY avg_complaints_per_hour DESC;
```

### 2. Correlation: Precipitation vs. Traffic Volume
```sql
SELECT 
  borough,
  CASE 
    WHEN precipitation_mm = 0 THEN 'No Rain'
    WHEN precipitation_mm > 0 AND precipitation_mm <= 5 THEN 'Light Rain'
    ELSE 'Heavy Rain'
  END AS rain_category,
  AVG(traffic_volume_count) AS avg_traffic,
  COUNT(*) AS hour_count
FROM gold_nyc_analytics
GROUP BY borough, rain_category
ORDER BY borough, rain_category;
```

### 3. Hourly patterns: Time of day vs. complaints
```sql
SELECT 
  HOUR(hour) AS hour_of_day,
  borough,
  AVG(complaints_311_count) AS avg_complaints,
  AVG(traffic_volume_count) AS avg_traffic,
  AVG(temp_celsius) AS avg_temp
FROM gold_nyc_analytics
GROUP BY HOUR(hour), borough
ORDER BY hour_of_day, borough;
```

---

##  Troubleshooting

### Issue: Bronze notebook fails with API timeout

**Cause:** API rate limiting or network latency
**Solution:**
- Increase retry count in `fetch_with_retry()` (check Bronze notebook)
- Add backoff delay between batches
- Check API status page

### Issue: Silver notebook has many quarantine rows

**Cause:** Data quality issues in source or unexpected schema changes
**Solution:**
1. Inspect quarantine table: `SELECT * FROM quarantine_weather LIMIT 10`
2. Identify common failure patterns
3. Adjust validation ranges in Silver notebook if needed
4. Re-run Silver notebook

### Issue: Gold notebook join produces fewer rows than expected

**Cause:** Some hours missing data (e.g., no 311 complaints)
**Solution:**
- Use **LEFT JOIN** (weather/traffic as base) instead of INNER JOIN
- This keeps all borough-hours even if one source is missing
- Check Gold_v1.ipynb for join strategy used

### Issue: Timestamps don't align across tables

**Cause:** Timezone or granularity misalignment
**Solution:**
- Ensure all timestamps are UTC (set in Bronze)
- Traffic: convert 15-min intervals to hourly (Silver)
- 311: truncate to hour in Gold join
- Check timestamp columns in each layer

---

##  Performance Tuning

### Spark Optimization

- **Partition by date:** Bronze/Silver tables partitioned by `ingestion_date` or `clean_date`
- **Z-order by borough:** Improves join performance on `borough` column
- **Cache intermediate DataFrames:** For multi-step Silver transformations

### Lakehouse Optimization

- **Use Delta cache:** Lakehouse auto-caches frequent columns
- **Write mode:** Append-only (no rewrites); ensures fast ingestion
- **Maintenance:** Run `OPTIMIZE` on tables after large appends

---

##  Security & Privacy

- **No PII storage:** 311 data contains location (lat/lon) and dates, but no names or contact info
- **API keys:** If storing API credentials, use Fabric Secrets (not shown in notebooks)
- **Data retention:** Define policies for quarantine table cleanup
- **Access control:** Use Fabric workspace roles (Viewer, Editor, Admin)

---

**Last Updated:** April 2026  
**Project:** MSBA 305 — Data Engineering with Microsoft Fabric  
**Architecture:** Medallion (Bronze → Silver → Gold)  
**Status:** Production-ready
