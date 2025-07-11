#!/bin/bash
# Script to download and analyze parquet files from S3

# Create tmp directory for parquet files if it doesn't exist
mkdir -p /tmp/parquet

# Set date and station code
DATE="2025-06-03 00:00:00"
STATION="DAU1"
S3_BASE="s3://altdatasetexfil/claudecloud/routing2_container_snip"
S3_PATH="${S3_BASE}/date=${DATE}/station_code=${STATION}"

# Download a sample parquet file
echo "Downloading sample parquet file..."
aws s3 cp "${S3_PATH}/0000_part_00.parquet" /tmp/parquet/

# Run the analysis using the conda environment
echo "Running analysis..."
cd /home/admsia/shipment_timeline/container_planning/analysis
/home/admsia/miniforge/bin/python analyze_parquet.py /tmp/parquet/0000_part_00.parquet