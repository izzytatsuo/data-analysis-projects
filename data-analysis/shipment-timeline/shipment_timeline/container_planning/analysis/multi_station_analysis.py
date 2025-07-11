#!/usr/bin/env python
"""
Multi-Station Analysis Script

This script analyzes container planning data from multiple stations to identify patterns,
compare performance, and generate insights about planning effectiveness.
"""

import os
import sys
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import boto3
import tempfile

# Set plot styling
plt.style.use('ggplot')
sns.set(style="whitegrid")
plt.rcParams['figure.figsize'] = [12, 8]

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 1000)

# Station codes to analyze
STATIONS = ['DAU1', 'DAU2', 'DAU5', 'DAU7', 'DJT6']
DATES = ['2025-06-01 00:00:00', '2025-06-02 00:00:00', '2025-06-03 00:00:00']
S3_BASE_PATH = 's3://altdatasetexfil/claudecloud/routing2_container_snip'
OUTPUT_DIR = '/home/admsia/shipment_timeline/container_planning/analysis/results'

def download_station_data(station_code, date, output_dir="/tmp/parquet"):
    """
    Download parquet data for a specific station and date.
    
    Args:
        station_code: The station code (e.g., 'DAU1')
        date: The date string in format 'YYYY-MM-DD 00:00:00'
        output_dir: Directory to save the downloaded file
        
    Returns:
        Path to the downloaded file, or None if not found
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        # Construct S3 path
        s3_path = f"{S3_BASE_PATH}/date={date}/station_code={station_code}/0000_part_00.parquet"
        output_file = f"{output_dir}/{station_code}_{date.split()[0]}.parquet"
        
        # Check if file exists
        s3 = boto3.client('s3')
        try:
            bucket = 'altdatasetexfil'
            key = f"claudecloud/routing2_container_snip/date={date}/station_code={station_code}/0000_part_00.parquet"
            s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"File not found for {station_code} on {date}: {e}")
            return None
            
        # Download file
        cmd = f"aws s3 cp {s3_path} {output_file}"
        print(f"Downloading with command: {cmd}")
        if os.system(cmd) != 0:
            print(f"Error downloading file for {station_code} on {date}")
            return None
            
        return output_file
    except Exception as e:
        print(f"Error downloading file for {station_code} on {date}: {e}")
        return None

def analyze_station_data(file_path):
    """
    Analyze parquet data for a specific station.
    
    Args:
        file_path: Path to the parquet file
        
    Returns:
        Dictionary with analysis results, or None if file can't be read
    """
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
            
        # Read the parquet file
        parquet_file = pq.read_table(file_path)
        df = parquet_file.to_pandas()
        
        # Extract station and date from filename
        file_name = os.path.basename(file_path)
        station_code = file_name.split('_')[0]
        date_str = file_name.split('_')[1].replace('.parquet', '')
        
        # Calculate planning metrics
        total_packages = len(df)
        planned_packages = df['is_planned'].sum() if 'is_planned' in df.columns else 0
        inducted_packages = df['is_inducted'].sum() if 'is_inducted' in df.columns else 0
        inducted_as_planned = df['is_inducted_as_planned'].sum() if 'is_inducted_as_planned' in df.columns else 0
        
        # Calculate percentages
        planning_rate = 100.0 * planned_packages / total_packages if total_packages > 0 else 0
        inducted_rate = 100.0 * inducted_packages / total_packages if total_packages > 0 else 0
        inducted_as_planned_rate = 100.0 * inducted_as_planned / inducted_packages if inducted_packages > 0 else 0
        no_cycle_rate = 100.0 * (inducted_packages - inducted_as_planned) / inducted_packages if inducted_packages > 0 else 0
        
        # Analyze originating nodes
        originating_nodes = df['originating_node'].nunique() if 'originating_node' in df.columns else 0
        
        # Analyze node distribution
        node_distribution = {}
        if 'originating_node' in df.columns:
            node_counts = df['originating_node'].value_counts()
            for node, count in node_counts.items():
                node_distribution[node] = {
                    'count': count,
                    'percentage': 100.0 * count / total_packages
                }
        
        # Timing analysis if timestamps are available
        timing_metrics = {}
        if all(col in df.columns for col in ['dcap_run_time_local', 'induct_datetime_local']):
            df['plan_to_induct_mins'] = (df['induct_datetime_local'] - df['dcap_run_time_local']).dt.total_seconds() / 60
            timing_metrics['plan_to_induct_mins'] = {
                'mean': df['plan_to_induct_mins'].mean(),
                'median': df['plan_to_induct_mins'].median(),
                'std': df['plan_to_induct_mins'].std()
            }
            
        if all(col in df.columns for col in ['induct_datetime_local', 'stow_datetime']):
            df['induct_to_stow_mins'] = (df['stow_datetime'] - df['induct_datetime_local']).dt.total_seconds() / 60
            timing_metrics['induct_to_stow_mins'] = {
                'mean': df['induct_to_stow_mins'].mean(),
                'median': df['induct_to_stow_mins'].median(),
                'std': df['induct_to_stow_mins'].std()
            }
        
        return {
            'station_code': station_code,
            'date': date_str,
            'total_packages': total_packages,
            'planned_packages': planned_packages,
            'inducted_packages': inducted_packages,
            'inducted_as_planned': inducted_as_planned,
            'planning_rate': planning_rate,
            'inducted_rate': inducted_rate,
            'inducted_as_planned_rate': inducted_as_planned_rate,
            'no_cycle_rate': no_cycle_rate,
            'originating_nodes': originating_nodes,
            'node_distribution': node_distribution,
            'timing_metrics': timing_metrics
        }
    except Exception as e:
        print(f"Error analyzing file {file_path}: {e}")
        return None

def generate_comparison_visuals(results, output_dir):
    """
    Generate visual comparisons between stations.
    
    Args:
        results: List of analysis results dictionaries
        output_dir: Directory to save visualizations
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a DataFrame from results
    df = pd.DataFrame(results)
    
    # Add calculated columns
    df['no_cycle_packages'] = df['inducted_packages'] - df['inducted_as_planned']
    
    # 1. Planning Rate Comparison
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x='station_code', y='planning_rate', hue='date', data=df)
    plt.title('Planning Rate by Station and Date')
    plt.xlabel('Station')
    plt.ylabel('Planning Rate (%)')
    plt.ylim(0, 100)
    plt.savefig(f"{output_dir}/planning_rate_comparison.png")
    
    # 2. No Cycle Rate Comparison
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x='station_code', y='no_cycle_rate', hue='date', data=df)
    plt.title('No Cycle Rate by Station and Date')
    plt.xlabel('Station')
    plt.ylabel('No Cycle Rate (%)')
    plt.ylim(0, 100)
    plt.savefig(f"{output_dir}/no_cycle_rate_comparison.png")
    
    # 3. Volume Comparison
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x='station_code', y='total_packages', hue='date', data=df)
    plt.title('Package Volume by Station and Date')
    plt.xlabel('Station')
    plt.ylabel('Total Packages')
    plt.savefig(f"{output_dir}/volume_comparison.png")
    
    # 4. Originating Node Count Comparison
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(x='station_code', y='originating_nodes', hue='date', data=df)
    plt.title('Originating Node Count by Station and Date')
    plt.xlabel('Station')
    plt.ylabel('Unique Originating Nodes')
    plt.savefig(f"{output_dir}/originating_nodes_comparison.png")
    
    # 5. Planning vs No-Cycle Correlation Plot
    plt.figure(figsize=(12, 8))
    ax = sns.scatterplot(x='planning_rate', y='no_cycle_rate', hue='station_code', 
                         size='total_packages', sizes=(50, 500), data=df)
    plt.title('Planning Rate vs No Cycle Rate')
    plt.xlabel('Planning Rate (%)')
    plt.ylabel('No Cycle Rate (%)')
    plt.grid(True)
    plt.savefig(f"{output_dir}/planning_vs_no_cycle.png")
    
    # 6. Time Series Analysis (if multiple dates)
    if len(df['date'].unique()) > 1:
        # Convert date strings to datetime
        df['date_dt'] = pd.to_datetime(df['date'])
        
        plt.figure(figsize=(14, 8))
        for station in df['station_code'].unique():
            station_data = df[df['station_code'] == station]
            plt.plot(station_data['date_dt'], station_data['no_cycle_rate'], 
                     marker='o', label=station)
        
        plt.title('No Cycle Rate Trend by Station')
        plt.xlabel('Date')
        plt.ylabel('No Cycle Rate (%)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{output_dir}/no_cycle_trend.png")
    
    print(f"Generated comparison visualizations in {output_dir}")
    
def generate_summary_report(results, output_dir):
    """
    Generate a markdown summary report of the analysis.
    
    Args:
        results: List of analysis results dictionaries
        output_dir: Directory to save the report
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a DataFrame from results for easier analysis
    df = pd.DataFrame(results)
    
    # Format the report
    report = "# Multi-Station Container Planning Analysis\n\n"
    report += f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += f"## Summary\n\n"
    report += f"This report analyzes container planning data across {len(df['station_code'].unique())} stations "
    report += f"and {len(df['date'].unique())} dates.\n\n"
    
    # Add key metrics table
    report += "## Key Metrics by Station\n\n"
    report += "| Station | Date | Total Packages | Planning Rate | No Cycle Rate | Unique Origin Nodes |\n"
    report += "|---------|------|----------------|---------------|---------------|---------------------|\n"
    
    for _, row in df.sort_values(['station_code', 'date']).iterrows():
        report += f"| {row['station_code']} | {row['date']} | {row['total_packages']} | "
        report += f"{row['planning_rate']:.2f}% | {row['no_cycle_rate']:.2f}% | {row['originating_nodes']} |\n"
    
    # Add planning effectiveness analysis
    report += "\n## Planning Effectiveness Analysis\n\n"
    
    # Best and worst stations
    if len(df) > 1:
        best_planning = df.loc[df['planning_rate'].idxmax()]
        worst_planning = df.loc[df['planning_rate'].idxmin()]
        best_no_cycle = df.loc[df['no_cycle_rate'].idxmin()]
        worst_no_cycle = df.loc[df['no_cycle_rate'].idxmax()]
        
        report += "### Performance Extremes\n\n"
        report += f"- Best Planning Rate: **{best_planning['station_code']}** on {best_planning['date']} ({best_planning['planning_rate']:.2f}%)\n"
        report += f"- Worst Planning Rate: **{worst_planning['station_code']}** on {worst_planning['date']} ({worst_planning['planning_rate']:.2f}%)\n"
        report += f"- Lowest No Cycle Rate: **{best_no_cycle['station_code']}** on {best_no_cycle['date']} ({best_no_cycle['no_cycle_rate']:.2f}%)\n"
        report += f"- Highest No Cycle Rate: **{worst_no_cycle['station_code']}** on {worst_no_cycle['date']} ({worst_no_cycle['no_cycle_rate']:.2f}%)\n\n"
    
    # Analysis of correlations
    report += "### Correlation Analysis\n\n"
    
    # Calculate correlation between planning rate and no cycle rate
    if len(df) > 2:
        correlation = df['planning_rate'].corr(df['no_cycle_rate'])
        report += f"Correlation between Planning Rate and No Cycle Rate: **{correlation:.3f}**\n\n"
        
        if correlation < -0.5:
            report += "There is a strong negative correlation between planning rate and no cycle rate, "
            report += "suggesting that higher planning rates lead to lower no cycle rates, as expected.\n\n"
        elif correlation > 0.5:
            report += "Unexpectedly, there is a positive correlation between planning rate and no cycle rate. "
            report += "This suggests other factors may be influencing the relationship.\n\n"
        else:
            report += "There is no strong correlation between planning rate and no cycle rate, "
            report += "suggesting that factors beyond just planning percentage affect no cycle outcomes.\n\n"
    
    # Add originating node analysis
    report += "## Originating Node Impact\n\n"
    
    # Calculate average no cycle rate by number of origin nodes
    if 'originating_nodes' in df.columns:
        df['node_bucket'] = pd.cut(df['originating_nodes'], bins=[0, 5, 10, 15, 100], 
                                  labels=['1-5 nodes', '6-10 nodes', '11-15 nodes', '16+ nodes'])
        node_impact = df.groupby('node_bucket')['no_cycle_rate'].mean().reset_index()
        
        report += "### No Cycle Rate by Number of Originating Nodes\n\n"
        report += "| Originating Node Count | Average No Cycle Rate |\n"
        report += "|------------------------|-----------------------|\n"
        
        for _, row in node_impact.iterrows():
            if not pd.isna(row['node_bucket']):
                report += f"| {row['node_bucket']} | {row['no_cycle_rate']:.2f}% |\n"
        
        report += "\n"
    
    # Add timing analysis
    report += "## Timing Analysis\n\n"
    
    # Extract and format timing metrics if available
    timing_data = []
    for result in results:
        if 'timing_metrics' in result and result['timing_metrics']:
            station = result['station_code']
            date = result['date']
            
            if 'plan_to_induct_mins' in result['timing_metrics']:
                metrics = result['timing_metrics']['plan_to_induct_mins']
                timing_data.append({
                    'station': station,
                    'date': date,
                    'metric': 'Plan to Induct',
                    'mean': metrics['mean'],
                    'median': metrics['median'],
                    'std': metrics['std']
                })
                
            if 'induct_to_stow_mins' in result['timing_metrics']:
                metrics = result['timing_metrics']['induct_to_stow_mins']
                timing_data.append({
                    'station': station,
                    'date': date,
                    'metric': 'Induct to Stow',
                    'mean': metrics['mean'],
                    'median': metrics['median'],
                    'std': metrics['std']
                })
    
    if timing_data:
        timing_df = pd.DataFrame(timing_data)
        report += "### Average Processing Times (minutes)\n\n"
        report += "| Station | Date | Metric | Mean (min) | Median (min) | Std Dev |\n"
        report += "|---------|------|--------|------------|--------------|--------|\n"
        
        for _, row in timing_df.iterrows():
            report += f"| {row['station']} | {row['date']} | {row['metric']} | "
            report += f"{row['mean']:.2f} | {row['median']:.2f} | {row['std']:.2f} |\n"
    
    # Add recommendations section
    report += "\n## Recommendations\n\n"
    
    # Generate general recommendations
    report += "Based on the analysis, here are key recommendations for improving planning effectiveness:\n\n"
    
    # Add station-specific recommendations
    for station in df['station_code'].unique():
        station_data = df[df['station_code'] == station].sort_values('date')
        
        if len(station_data) > 0:
            latest = station_data.iloc[-1]
            report += f"### {station} Recommendations\n\n"
            
            if latest['no_cycle_rate'] > 20:
                report += f"- **High Priority**: Reduce no-cycle rate (currently {latest['no_cycle_rate']:.2f}%)\n"
                if latest['planning_rate'] < 80:
                    report += f"  - Improve planning rate (currently {latest['planning_rate']:.2f}%)\n"
                
                if latest['originating_nodes'] > 10:
                    report += f"  - Analyze impact of high originating node count ({latest['originating_nodes']})\n"
            elif latest['no_cycle_rate'] > 10:
                report += f"- **Medium Priority**: Monitor no-cycle rate (currently {latest['no_cycle_rate']:.2f}%)\n"
            else:
                report += f"- **Best Practice Example**: Maintain current performance (no-cycle rate: {latest['no_cycle_rate']:.2f}%)\n"
                report += "  - Document processes for knowledge sharing with other stations\n"
            
            # Add time-based recommendations if available
            for result in results:
                if result['station_code'] == station and 'timing_metrics' in result and result['timing_metrics']:
                    if 'plan_to_induct_mins' in result['timing_metrics']:
                        plan_to_induct = result['timing_metrics']['plan_to_induct_mins']['mean']
                        if plan_to_induct > 120:  # If more than 2 hours
                            report += f"  - Reduce time from planning to induction (currently {plan_to_induct:.2f} minutes)\n"
                    break
            
            report += "\n"
    
    # Save the report
    report_path = f"{output_dir}/multi_station_analysis_report.md"
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Generated summary report at {report_path}")
    return report_path

def main():
    """Main function to run the multi-station analysis."""
    print("Starting multi-station container planning analysis...")
    
    # Create the output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    results = []
    
    # Download and analyze data for each station and date
    for station in STATIONS:
        for date in DATES:
            print(f"\nProcessing {station} on {date}...")
            
            # Download the data
            file_path = download_station_data(station, date)
            if file_path:
                print(f"Downloaded file: {file_path}")
                
                # Analyze the data
                result = analyze_station_data(file_path)
                if result:
                    results.append(result)
                    print(f"Analysis completed for {station} on {date}")
                else:
                    print(f"Failed to analyze data for {station} on {date}")
            else:
                print(f"Failed to download data for {station} on {date}")
    
    # Generate comparisons and report if we have results
    if results:
        print("\nGenerating comparison visualizations...")
        generate_comparison_visuals(results, OUTPUT_DIR)
        
        print("\nGenerating summary report...")
        report_path = generate_summary_report(results, OUTPUT_DIR)
        
        print(f"\nAnalysis complete. Results saved to {OUTPUT_DIR}")
        print(f"Summary report: {report_path}")
    else:
        print("\nNo results to analyze. Check the station codes and dates.")
    
if __name__ == "__main__":
    main()