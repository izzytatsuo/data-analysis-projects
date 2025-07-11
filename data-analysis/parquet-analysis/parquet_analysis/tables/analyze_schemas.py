import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import os
import sys
from io import BytesIO

def analyze_table_schema(table_name):
    """Analyze just the schema of a table from S3 without downloading the entire file"""
    s3 = boto3.client('s3')
    bucket = "altdatasetexfil"
    base_path = f"dnet/backfill/{table_name}"
    
    print(f"\nAnalyzing schema for {table_name}...")
    
    try:
        # List partitions
        response = s3.list_objects_v2(Bucket=bucket, Prefix=base_path, Delimiter='/')
        if 'CommonPrefixes' not in response:
            print(f"No partitions found for {table_name}")
            return
        
        # Use the first partition
        partition_prefix = response['CommonPrefixes'][0]['Prefix']
        
        # List files in the partition
        file_response = s3.list_objects_v2(Bucket=bucket, Prefix=partition_prefix, MaxKeys=1)
        if 'Contents' not in file_response or len(file_response['Contents']) == 0:
            print(f"No files found in partition {partition_prefix}")
            return
        
        # Get the first file
        file_key = file_response['Contents'][0]['Key']
        print(f"Reading schema from s3://{bucket}/{file_key}")
        
        # Read just the metadata from the file
        buffer = BytesIO()
        s3.download_fileobj(Bucket=bucket, Key=file_key, Fileobj=buffer)
        buffer.seek(0)
        
        # Read schema only
        parquet_file = pq.ParquetFile(buffer)
        schema = parquet_file.schema_arrow
        
        # Create SQL schema file
        sql_file_path = f"/local/home/admsia/parquet_analysis/tables/{table_name}_schema.sql"
        with open(sql_file_path, 'w') as f:
            f.write(f"-- Schema for {table_name}\n")
            f.write(f"CREATE TABLE {table_name} (\n")
            
            column_defs = []
            print("\nColumns:")
            for i, field in enumerate(schema):
                field_name = field.name
                field_type = field.type
                
                # Map Arrow types to SQL types
                sql_type = "VARCHAR"
                if pa.types.is_integer(field_type):
                    sql_type = "INTEGER"
                elif pa.types.is_floating(field_type):
                    sql_type = "DECIMAL"
                elif pa.types.is_boolean(field_type):
                    sql_type = "BOOLEAN"
                elif pa.types.is_timestamp(field_type):
                    sql_type = "TIMESTAMP"
                elif pa.types.is_date(field_type):
                    sql_type = "DATE"
                elif pa.types.is_binary(field_type):
                    sql_type = "BLOB"
                
                print(f"  {field_name}: {field_type}")
                column_defs.append(f"  {field_name} {sql_type}")
            
            f.write(",\n".join(column_defs))
            f.write("\n);\n")
        
        print(f"\nSchema written to {sql_file_path}")
        
    except Exception as e:
        print(f"Error analyzing {table_name}: {str(e)}")

# Tables to analyze
tables = [
    "gmp_shipment_events_na",
    "induct_events_na", 
    "o_slam_packages_leg_live",
    "o_slam_packages_live",
    "package_systems_event_na"
]

# Analyze each table
for table in tables:
    analyze_table_schema(table)