# S3 Database Schema Analysis Session Summary

This document summarizes the work done in analyzing table schemas from S3 data locations.

## Tables Analyzed

### 1. v_load_summary_hourly
**Source:** `s3://altdatasetexfil/V_LOAD_SUMMARY_HOURLY/`
**Schema File:** `/local/home/admsia/parquet_analysis/v_load_summary_hourly_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-1.ipynb`

Key insights:
- 116 columns with vehicle route information
- Includes timing data (arrivals, departures, transit hours)
- Contains geographic information (origin, destination)
- Has cost and shipment metrics
- Performance metrics (on-time vs. late)

### 2. gmp_shipment_events_na
**Source:** `s3://altdatasetexfil/dnet/backfill/gmp_shipment_events_na/`
**Schema File:** `/local/home/admsia/parquet_analysis/tables/gmp_shipment_events_na_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-2.ipynb`

Key insights:
- 116 columns of shipment tracking events
- Contains tracking IDs, status codes, and timestamps
- Includes geographic information about pickup and delivery
- Has package dimensions and identifiers

### 3. induct_events_na
**Source:** `s3://altdatasetexfil/dnet/backfill/induct_events_na/`
**Schema File:** `/local/home/admsia/parquet_analysis/tables/induct_events_na_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-2.ipynb`

Key insights:
- 26 columns for package induction events
- Contains entity IDs, event types, and timestamps
- Has routing information and destinations
- Includes tracking IDs and sorting details

### 4. o_slam_packages_leg_live
**Source:** `s3://altdatasetexfil/dnet/backfill/o_slam_packages_leg_live/`
**Schema File:** `/local/home/admsia/parquet_analysis/tables/o_slam_packages_leg_live_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-2.ipynb`

Key insights:
- 45 columns for package shipment leg details
- Contains package IDs, route information, and warehouse data
- Has cost data, transit times, and weights
- Includes sequence IDs for multi-leg routes

### 5. o_slam_packages_live
**Source:** `s3://altdatasetexfil/dnet/backfill/o_slam_packages_live/`
**Schema File:** `/local/home/admsia/parquet_analysis/tables/o_slam_packages_live_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-2.ipynb`

Key insights:
- 56 columns for package shipment details
- Contains package IDs, address information, and shipping methods
- Has cost data, dimensions, and weights
- Includes promised and estimated delivery dates

### 6. package_systems_event_na
**Source:** `s3://altdatasetexfil/dnet/backfill/package_systems_event_na/`
**Schema File:** `/local/home/admsia/parquet_analysis/tables/package_systems_event_na_schema.sql`
**Analysis Notebook:** `/home/admsia/Untitled-2.ipynb`

Key insights:
- 27 columns for package event data
- Contains package IDs, tracking IDs, and status information
- Has location data and event timestamps
- Includes both forward and reverse tracking information

## Key Files Created

1. **v_load_summary_hourly_schema.sql**
   - Full SQL schema definition for load summary table
   - Located at: `/local/home/admsia/parquet_analysis/v_load_summary_hourly_schema.sql`

2. **Table-specific schema files**
   - SQL schemas for all 5 tables from dnet/backfill
   - Located in: `/local/home/admsia/parquet_analysis/tables/`

3. **Analysis Notebooks**
   - v_load_summary_hourly analysis: `/home/admsia/Untitled-1.ipynb`
   - dnet/backfill tables analysis: `/home/admsia/Untitled-2.ipynb`

## Table Relationships

The tables are related through several common fields:

1. Package Identification:
   - `gmp_shipment_events_na.package_id` relates to `o_slam_packages_live.package_id`
   - `o_slam_packages_leg_live.package_id` relates to `o_slam_packages_live.package_id`
   - `package_systems_event_na.package_id` relates to all package IDs in other tables

2. Tracking Information:
   - `gmp_shipment_events_na.tracking_id` relates to `induct_events_na.tracking_id`
   - `gmp_shipment_events_na.tracking_id` relates to `package_systems_event_na.forward_tracking_id`

3. Routing Information:
   - `o_slam_packages_leg_live.route_id` relates to `o_slam_packages_live.route_id`
   - `o_slam_packages_leg_live.route_id` relates to `induct_events_na.route_id`

4. Vehicle Information:
   - `v_load_summary_hourly` contains the high-level vehicle/load information
   - This can be connected to package-level tables via facility and route identifiers

## Python Code for Analysis

Python code for analyzing the parquet files:

```python
import pandas as pd
import pyarrow.parquet as pq

# Read schema only
file_path = "/path/to/parquet/file.parquet"
schema = pq.read_schema(file_path)
print("Table Schema:")
for field in schema:
    print(f"  {field.name}: {field.type}")

# Read data with pandas
df = pd.read_parquet(file_path)
print(f"Data shape: {df.shape}")
print(df.head())

# Basic statistics
print(df.describe())
```

## Commands Used

Major commands used during this session:

```bash
# List S3 directories
aws s3 ls s3://altdatasetexfil/dnet/backfill/

# Download sample parquet file
aws s3 cp s3://altdatasetexfil/dnet/backfill/gmp_shipment_events_na/partition_date=2025-06-02\ 00\:00\:00/0000_part_00.parquet /local/home/admsia/parquet_analysis/tables/gmp_shipment_events_na.parquet

# Install Python packages
conda install -y boto3 pandas pyarrow

# Analyze parquet schema with Python
python analyze_schemas.py
```