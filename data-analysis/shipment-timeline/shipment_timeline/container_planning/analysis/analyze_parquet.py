#!/usr/bin/env python
"""
Analyze parquet files from container planning data.
This script reads parquet files and analyzes their structure and content.
"""

import pyarrow.parquet as pq
import pandas as pd
import sys
import os
from datetime import datetime

def analyze_parquet(file_path):
    """
    Analyze a parquet file and print its schema and sample data.
    
    Args:
        file_path: Path to the parquet file
    """
    print(f"Analyzing parquet file: {file_path}")
    try:
        # Read the parquet file
        parquet_file = pq.read_table(file_path)
        df = parquet_file.to_pandas()
        
        # Print schema
        print("\nSchema:")
        for col in parquet_file.schema:
            print(f"- {col.name}: {col.type}")
        
        # Print sample data
        print("\nSample Data (5 rows):")
        print(df.head(5).to_string())
        
        # Print some statistics
        print("\nStatistics:")
        print(f"- Total rows: {len(df)}")
        print(f"- Unique originating nodes: {df['originating_node'].nunique()}")
        if 'is_planned' in df.columns:
            print(f"- Planned packages: {df['is_planned'].sum()}")
        if 'is_inducted' in df.columns:
            print(f"- Inducted packages: {df['is_inducted'].sum()}")
        if 'is_inducted_as_planned' in df.columns:
            print(f"- Inducted as planned: {df['is_inducted_as_planned'].sum()}")
        
        return df
    except Exception as e:
        print(f"Error analyzing parquet file: {e}")
        return None

def analyze_directory(dir_path):
    """
    Analyze all parquet files in a directory.
    
    Args:
        dir_path: Path to directory containing parquet files
    """
    files = [f for f in os.listdir(dir_path) if f.endswith('.parquet')]
    
    if not files:
        print(f"No parquet files found in {dir_path}")
        return None
    
    print(f"Found {len(files)} parquet files in {dir_path}")
    
    # Analyze the first file
    first_file = os.path.join(dir_path, files[0])
    return analyze_parquet(first_file)

def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python analyze_parquet.py <path_to_parquet_file_or_directory>")
        return
    
    path = sys.argv[1]
    
    if os.path.isdir(path):
        analyze_directory(path)
    elif os.path.isfile(path):
        analyze_parquet(path)
    else:
        print(f"Path does not exist: {path}")

if __name__ == "__main__":
    main()