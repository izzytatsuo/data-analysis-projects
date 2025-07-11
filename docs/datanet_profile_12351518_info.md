# Datanet Profile 12351518: AMZLPRODDB - Backlog Denormalized Dataset V2

## Profile Overview
- **ID**: 12351518
- **Type**: TRANSFORM
- **Revision**: 43
- **Status**: ACTIVE
- **Last Updated By**: shransh
- **Last Updated**: 2024-11-27

## SQL Details
This is a complex ETL transformation job that processes Amazon Logistics (AMZL) backlog data. The SQL query:
- Contains 961 lines (0.05 MB)
- Creates multiple temporary tables for staging data
- References multiple source tables from various databases including:
  - andes.perfectmile
  - backlog_datasets.ATROPS
  - backlog_datasets.amzlcore
  - andes.amzl_analytics
- Processes package tracking information, shipment events, and delivery station data
- Calculates backlog metrics by delivery station and package status
- Unloads data to S3 destinations

## Associated Job
- **Job ID**: 26565528
- **Type**: TRANSFORM
- **Owner**: shransh
- **Group**: AMZLPRODDB-ORBIT-DE
- **Logical DB**: amzlproddb
- **Schedule Type**: INTRADAY (runs hourly)
- **Status**: ACTIVE
- **DB User**: amzn:cdo:datanet-dbuser:amzlproddb_rs_etl

## Latest Job Run
- **Run ID**: 11080185307
- **Status**: WAITING_FOR_REQUIREMENTS
- **Dataset Date**: 2025-07-06
- **Interval**: 2025-07-06T17:00:00Z to 2025-07-06T18:00:00Z
- **Result Location**: /dss/dwp/data/backlog_denorm_dataset_v2_20250706170000_20250706180000_NA.txt
- **Result ARN**: arn:amazon:edx:iad::manifest/etlm/backlog-denorm-dataset-v2/r-1-hourly/[2025-07-06T10:00:00-07:00]

## Key Process Steps
1. Retrieves delivery station mapping information
2. Processes SLAM (Ship Label Application Machine) data
3. Captures package status events from multiple systems
4. Identifies sidelines, truck check-ins, and node types
5. Computes aggregated backlog metrics
6. Unloads final data to S3 in multiple destinations including:
   - s3://orbit-de-prod/backlog_aggregated_hourly_unload/
   - s3://orbit-de-prod/backlog_aggregated_hourly_unload_backfill_backup/
   - s3://orbit-de-prod/backlog_aggregated_hourly_unload_proddb/
