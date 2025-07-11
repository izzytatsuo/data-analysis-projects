#!/bin/bash
# Script to download and analyze parquet files for DJT6 on 2025-06-02

# Create tmp directory for parquet files if it doesn't exist
mkdir -p /tmp/parquet

# Set date and station code
DATE="2025-06-02 00:00:00"
STATION="DJT6"
S3_BASE="s3://altdatasetexfil/claudecloud/routing2_container_snip"
S3_PATH="${S3_BASE}/date=${DATE}/station_code=${STATION}"

# List available files for this station and date
echo "Checking for files in ${S3_PATH}..."
aws s3 ls "${S3_PATH}/" | head -n 5

# Download a sample parquet file
echo "Downloading sample parquet file..."
aws s3 cp "${S3_PATH}/0000_part_00.parquet" /tmp/parquet/djt6_sample.parquet

# Run the analysis using the conda environment
echo "Running analysis..."
/home/admsia/miniforge/bin/python /home/admsia/shipment_timeline/container_planning/analysis/analyze_parquet.py /tmp/parquet/djt6_sample.parquet