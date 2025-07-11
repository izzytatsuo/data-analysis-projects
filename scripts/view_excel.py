#!/usr/bin/env python
"""
Script to read and display contents of an Excel file
Usage: /home/admsia/miniforge/bin/python view_excel.py /path/to/your/file.xlsx
"""

import sys
import pandas as pd

def view_excel(file_path):
    """Read and display contents of an Excel file."""
    try:
        # Read the Excel file
        print(f"Reading Excel file: {file_path}")
        
        # Get sheet names
        xlsx = pd.ExcelFile(file_path)
        sheet_names = xlsx.sheet_names
        print(f"\nSheet names: {sheet_names}")
        
        # Read each sheet
        for sheet_name in sheet_names:
            print(f"\n{'=' * 40}")
            print(f"Sheet: {sheet_name}")
            print(f"{'=' * 40}\n")
            
            # Read the sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Print sheet info
            print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print("\nColumns:")
            for col in df.columns:
                print(f"  - {col}")
            
            # Print first few rows
            print("\nFirst 5 rows:")
            print(df.head().to_string())
            
            # Print summary statistics for numeric columns
            print("\nSummary statistics:")
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                print(df[numeric_cols].describe().to_string())
            else:
                print("No numeric columns found")
                
    except Exception as e:
        print(f"Error reading Excel file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python view_excel.py /path/to/your/file.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    view_excel(file_path)