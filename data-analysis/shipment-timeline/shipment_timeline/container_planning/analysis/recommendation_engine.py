#!/usr/bin/env python
"""
Recommendation Engine for Container Planning Improvements

This script analyzes container planning data and generates actionable recommendations
for improving planning effectiveness and reducing no-cycle occurrences.
"""

import os
import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# Define constants
RESULTS_DIR = '/home/admsia/shipment_timeline/container_planning/analysis/results'
OUTPUT_DIR = '/home/admsia/shipment_timeline/container_planning/analysis/recommendations'

# Define thresholds for recommendations
THRESHOLDS = {
    'planning_rate': {
        'excellent': 95.0,  # 95% or higher is excellent
        'good': 85.0,       # 85-95% is good
        'average': 75.0,    # 75-85% is average
        'poor': 60.0        # Below 60% is poor
    },
    'no_cycle_rate': {
        'excellent': 5.0,   # 0-5% is excellent
        'good': 10.0,       # 5-10% is good
        'average': 20.0,    # 10-20% is average
        'poor': 30.0        # Above 30% is poor
    },
    'volume_threshold': 1000,  # Minimum volume for reliable metrics
    'improvement_delta': 5.0,   # Min percentage point improvement to consider significant
    'anomaly_std_multiplier': 2.0  # Standard deviations for anomaly detection
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_station_data(results_dir):
    """
    Load station data from analysis results.
    
    Args:
        results_dir: Directory containing analysis results
        
    Returns:
        DataFrame containing station metrics
    """
    # Try to load from multi-station analysis output
    report_path = os.path.join(results_dir, 'multi_station_analysis_report.md')
    if os.path.exists(report_path):
        return extract_data_from_report(report_path)
    
    # Fall back to sample data if no report exists
    return generate_sample_data()

def extract_data_from_report(report_path):
    """
    Extract data from multi-station analysis report.
    
    Args:
        report_path: Path to analysis report
        
    Returns:
        DataFrame with station metrics
    """
    try:
        with open(report_path, 'r') as f:
            content = f.read()
            
        # Extract table data
        table_start = content.find("| Station | Date | Total Packages |")
        if table_start == -1:
            return generate_sample_data()
            
        table_end = content.find("\n\n##", table_start)
        if table_end == -1:
            table_end = len(content)
            
        table_text = content[table_start:table_end]
        lines = [line for line in table_text.split('\n') if line.strip() and '|' in line]
        
        # Skip header and separator lines
        data_lines = lines[2:]
        
        # Parse table data
        data = []
        for line in data_lines:
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            if len(cells) >= 6:
                try:
                    row = {
                        'station_code': cells[0],
                        'date': cells[1],
                        'total_packages': int(cells[2]),
                        'planning_rate': float(cells[3].replace('%', '')),
                        'no_cycle_rate': float(cells[4].replace('%', '')),
                        'originating_nodes': int(cells[5])
                    }
                    data.append(row)
                except (ValueError, IndexError) as e:
                    print(f"Error parsing line: {line}, error: {e}")
        
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error extracting data from report: {e}")
        return generate_sample_data()

def generate_sample_data():
    """
    Generate sample station data for demonstration.
    
    Returns:
        DataFrame with sample station metrics
    """
    # Sample stations and dates
    stations = ['DAU1', 'DAU2', 'DAU5', 'DAU7', 'DJT6']
    dates = ['2025-06-01', '2025-06-02', '2025-06-03']
    
    # Station profiles
    station_profiles = {
        'DAU1': {'planning_base': 95, 'no_cycle_base': 5, 'volume_base': 5000, 'nodes_base': 6},
        'DAU2': {'planning_base': 85, 'no_cycle_base': 15, 'volume_base': 4500, 'nodes_base': 8},
        'DAU5': {'planning_base': 75, 'no_cycle_base': 20, 'volume_base': 3800, 'nodes_base': 10},
        'DAU7': {'planning_base': 80, 'no_cycle_base': 18, 'volume_base': 4200, 'nodes_base': 7},
        'DJT6': {'planning_base': 60, 'no_cycle_base': 35, 'volume_base': 6000, 'nodes_base': 13}
    }
    
    # Generate data
    data = []
    for station in stations:
        profile = station_profiles[station]
        for date in dates:
            # Add variations
            planning_rate = profile['planning_base'] + np.random.uniform(-5, 5)
            no_cycle_rate = profile['no_cycle_base'] + np.random.uniform(-3, 3)
            total_packages = int(profile['volume_base'] * np.random.uniform(0.9, 1.1))
            originating_nodes = int(profile['nodes_base'] * np.random.uniform(0.9, 1.1))
            
            data.append({
                'station_code': station,
                'date': date,
                'total_packages': total_packages,
                'planning_rate': planning_rate,
                'no_cycle_rate': no_cycle_rate,
                'originating_nodes': originating_nodes
            })
    
    return pd.DataFrame(data)

def analyze_station_trends(df):
    """
    Analyze trends for each station over time.
    
    Args:
        df: DataFrame with station metrics
        
    Returns:
        DataFrame with trend analysis
    """
    trend_data = []
    
    for station in df['station_code'].unique():
        station_df = df[df['station_code'] == station].sort_values('date')
        
        if len(station_df) < 2:
            # Need at least 2 points for trend analysis
            continue
            
        # Calculate changes from first to last date
        first = station_df.iloc[0]
        last = station_df.iloc[-1]
        
        planning_change = last['planning_rate'] - first['planning_rate']
        no_cycle_change = last['no_cycle_rate'] - first['no_cycle_rate']
        
        trend_data.append({
            'station_code': station,
            'start_date': first['date'],
            'end_date': last['date'],
            'planning_rate_start': first['planning_rate'],
            'planning_rate_end': last['planning_rate'],
            'planning_rate_change': planning_change,
            'no_cycle_rate_start': first['no_cycle_rate'],
            'no_cycle_rate_end': last['no_cycle_rate'],
            'no_cycle_rate_change': no_cycle_change,
            'planning_trend': 'improving' if planning_change > 0 else 'declining',
            'no_cycle_trend': 'improving' if no_cycle_change < 0 else 'declining',
            'latest_volume': last['total_packages']
        })
    
    return pd.DataFrame(trend_data)

def identify_best_practices(df):
    """
    Identify best practice stations based on metrics.
    
    Args:
        df: DataFrame with station metrics
        
    Returns:
        Dictionary of best practice stations and their metrics
    """
    # Only consider stations with sufficient volume
    high_volume_df = df[df['total_packages'] >= THRESHOLDS['volume_threshold']]
    
    if high_volume_df.empty:
        return {}
        
    # Get the latest date for each station
    latest_df = high_volume_df.sort_values('date').groupby('station_code').last().reset_index()
    
    # Find best stations for different metrics
    best_planning = latest_df.loc[latest_df['planning_rate'].idxmax()]
    best_no_cycle = latest_df.loc[latest_df['no_cycle_rate'].idxmin()]
    
    # Only consider as best practice if meeting the threshold
    best_practices = {}
    
    if best_planning['planning_rate'] >= THRESHOLDS['planning_rate']['excellent']:
        best_practices['planning_rate'] = {
            'station_code': best_planning['station_code'],
            'value': best_planning['planning_rate'],
            'date': best_planning['date']
        }
    
    if best_no_cycle['no_cycle_rate'] <= THRESHOLDS['no_cycle_rate']['excellent']:
        best_practices['no_cycle_rate'] = {
            'station_code': best_no_cycle['station_code'],
            'value': best_no_cycle['no_cycle_rate'],
            'date': best_no_cycle['date']
        }
    
    return best_practices

def detect_anomalies(df):
    """
    Detect anomalous performance patterns.
    
    Args:
        df: DataFrame with station metrics
        
    Returns:
        List of anomaly descriptions
    """
    anomalies = []
    
    # Calculate global statistics
    mean_planning = df['planning_rate'].mean()
    std_planning = df['planning_rate'].std()
    
    mean_no_cycle = df['no_cycle_rate'].mean()
    std_no_cycle = df['no_cycle_rate'].std()
    
    # Set anomaly thresholds
    planning_high = mean_planning + THRESHOLDS['anomaly_std_multiplier'] * std_planning
    planning_low = mean_planning - THRESHOLDS['anomaly_std_multiplier'] * std_planning
    
    no_cycle_high = mean_no_cycle + THRESHOLDS['anomaly_std_multiplier'] * std_no_cycle
    no_cycle_low = mean_no_cycle - THRESHOLDS['anomaly_std_multiplier'] * std_no_cycle
    
    # Check for anomalies
    for _, row in df.iterrows():
        # Exceptionally high planning rate
        if row['planning_rate'] > planning_high:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'planning_rate',
                'value': row['planning_rate'],
                'threshold': planning_high,
                'type': 'positive',
                'description': f"Exceptionally high planning rate ({row['planning_rate']:.2f}%)"
            })
        
        # Exceptionally low planning rate
        if row['planning_rate'] < planning_low:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'planning_rate',
                'value': row['planning_rate'],
                'threshold': planning_low,
                'type': 'negative',
                'description': f"Exceptionally low planning rate ({row['planning_rate']:.2f}%)"
            })
            
        # Exceptionally high no-cycle rate
        if row['no_cycle_rate'] > no_cycle_high:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'no_cycle_rate',
                'value': row['no_cycle_rate'],
                'threshold': no_cycle_high,
                'type': 'negative',
                'description': f"Exceptionally high no-cycle rate ({row['no_cycle_rate']:.2f}%)"
            })
            
        # Exceptionally low no-cycle rate
        if row['no_cycle_rate'] < no_cycle_low:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'no_cycle_rate',
                'value': row['no_cycle_rate'],
                'threshold': no_cycle_low,
                'type': 'positive',
                'description': f"Exceptionally low no-cycle rate ({row['no_cycle_rate']:.2f}%)"
            })
            
        # Correlation anomalies
        if row['planning_rate'] > THRESHOLDS['planning_rate']['excellent'] and row['no_cycle_rate'] > THRESHOLDS['no_cycle_rate']['average']:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'correlation',
                'type': 'unexpected',
                'description': f"High planning rate ({row['planning_rate']:.2f}%) with unexpectedly high no-cycle rate ({row['no_cycle_rate']:.2f}%)"
            })
            
        if row['planning_rate'] < THRESHOLDS['planning_rate']['poor'] and row['no_cycle_rate'] < THRESHOLDS['no_cycle_rate']['good']:
            anomalies.append({
                'station_code': row['station_code'],
                'date': row['date'],
                'metric': 'correlation',
                'type': 'unexpected',
                'description': f"Low planning rate ({row['planning_rate']:.2f}%) with unexpectedly low no-cycle rate ({row['no_cycle_rate']:.2f}%)"
            })
    
    return anomalies

def generate_recommendations(df, trend_df, best_practices, anomalies):
    """
    Generate prioritized improvement recommendations.
    
    Args:
        df: DataFrame with station metrics
        trend_df: DataFrame with trend analysis
        best_practices: Dictionary of best practice stations
        anomalies: List of anomalies
        
    Returns:
        Dictionary with prioritized recommendations
    """
    recommendations = {
        'high_priority': [],
        'medium_priority': [],
        'low_priority': [],
        'knowledge_sharing': [],
        'best_practices': []
    }
    
    # Get latest metrics for each station
    latest_df = df.sort_values('date').groupby('station_code').last().reset_index()
    
    # Process each station
    for _, row in latest_df.iterrows():
        station = row['station_code']
        
        # Skip stations with insufficient volume
        if row['total_packages'] < THRESHOLDS['volume_threshold']:
            continue
            
        # Get station trend data if available
        station_trend = None
        for _, trend in trend_df.iterrows():
            if trend['station_code'] == station:
                station_trend = trend
                break
        
        # High priority recommendations (no-cycle > 30% or planning < 60%)
        if row['no_cycle_rate'] > THRESHOLDS['no_cycle_rate']['poor']:
            rec = {
                'station_code': station,
                'metric': 'no_cycle_rate',
                'current_value': row['no_cycle_rate'],
                'target_value': THRESHOLDS['no_cycle_rate']['average'],
                'potential_impact': 'high',
                'recommendation': f"Reduce no-cycle rate from {row['no_cycle_rate']:.2f}% to below {THRESHOLDS['no_cycle_rate']['average']:.2f}%"
            }
            
            # Add trend context if available
            if station_trend is not None:
                if station_trend['no_cycle_trend'] == 'improving':
                    rec['context'] = f"Current trend is positive ({station_trend['no_cycle_rate_change']:.2f}% improvement)"
                else:
                    rec['context'] = f"Current trend is negative ({abs(station_trend['no_cycle_rate_change']):.2f}% deterioration)"
            
            recommendations['high_priority'].append(rec)
        
        if row['planning_rate'] < THRESHOLDS['planning_rate']['poor']:
            rec = {
                'station_code': station,
                'metric': 'planning_rate',
                'current_value': row['planning_rate'],
                'target_value': THRESHOLDS['planning_rate']['average'],
                'potential_impact': 'high',
                'recommendation': f"Increase planning rate from {row['planning_rate']:.2f}% to above {THRESHOLDS['planning_rate']['average']:.2f}%"
            }
            
            # Add trend context if available
            if station_trend is not None:
                if station_trend['planning_trend'] == 'improving':
                    rec['context'] = f"Current trend is positive ({station_trend['planning_rate_change']:.2f}% improvement)"
                else:
                    rec['context'] = f"Current trend is negative ({abs(station_trend['planning_rate_change']):.2f}% deterioration)"
            
            recommendations['high_priority'].append(rec)
        
        # Medium priority recommendations (no-cycle 10-30% or planning 60-75%)
        elif THRESHOLDS['no_cycle_rate']['good'] < row['no_cycle_rate'] <= THRESHOLDS['no_cycle_rate']['poor']:
            rec = {
                'station_code': station,
                'metric': 'no_cycle_rate',
                'current_value': row['no_cycle_rate'],
                'target_value': THRESHOLDS['no_cycle_rate']['good'],
                'potential_impact': 'medium',
                'recommendation': f"Reduce no-cycle rate from {row['no_cycle_rate']:.2f}% to below {THRESHOLDS['no_cycle_rate']['good']:.2f}%"
            }
            recommendations['medium_priority'].append(rec)
        
        elif THRESHOLDS['planning_rate']['poor'] <= row['planning_rate'] < THRESHOLDS['planning_rate']['average']:
            rec = {
                'station_code': station,
                'metric': 'planning_rate',
                'current_value': row['planning_rate'],
                'target_value': THRESHOLDS['planning_rate']['good'],
                'potential_impact': 'medium',
                'recommendation': f"Increase planning rate from {row['planning_rate']:.2f}% to above {THRESHOLDS['planning_rate']['good']:.2f}%"
            }
            recommendations['medium_priority'].append(rec)
        
        # Low priority recommendations (no-cycle 5-10% or planning 75-85%)
        elif THRESHOLDS['no_cycle_rate']['excellent'] < row['no_cycle_rate'] <= THRESHOLDS['no_cycle_rate']['good']:
            rec = {
                'station_code': station,
                'metric': 'no_cycle_rate',
                'current_value': row['no_cycle_rate'],
                'target_value': THRESHOLDS['no_cycle_rate']['excellent'],
                'potential_impact': 'low',
                'recommendation': f"Fine-tune no-cycle rate from {row['no_cycle_rate']:.2f}% to below {THRESHOLDS['no_cycle_rate']['excellent']:.2f}%"
            }
            recommendations['low_priority'].append(rec)
        
        elif THRESHOLDS['planning_rate']['average'] <= row['planning_rate'] < THRESHOLDS['planning_rate']['good']:
            rec = {
                'station_code': station,
                'metric': 'planning_rate',
                'current_value': row['planning_rate'],
                'target_value': THRESHOLDS['planning_rate']['excellent'],
                'potential_impact': 'low',
                'recommendation': f"Fine-tune planning rate from {row['planning_rate']:.2f}% to above {THRESHOLDS['planning_rate']['excellent']:.2f}%"
            }
            recommendations['low_priority'].append(rec)
        
        # Identify best practice examples
        if row['planning_rate'] >= THRESHOLDS['planning_rate']['excellent'] and row['no_cycle_rate'] <= THRESHOLDS['no_cycle_rate']['excellent']:
            rec = {
                'station_code': station,
                'planning_rate': row['planning_rate'],
                'no_cycle_rate': row['no_cycle_rate'],
                'recommendation': f"Document best practices from {station} for knowledge sharing"
            }
            recommendations['best_practices'].append(rec)
    
    # Add knowledge sharing recommendations based on best practices
    if 'no_cycle_rate' in best_practices and recommendations['high_priority']:
        best_station = best_practices['no_cycle_rate']['station_code']
        for rec in recommendations['high_priority']:
            if rec['metric'] == 'no_cycle_rate' and rec['station_code'] != best_station:
                knowledge_rec = {
                    'source_station': best_station,
                    'target_station': rec['station_code'],
                    'metric': 'no_cycle_rate',
                    'recommendation': f"Implement knowledge transfer from {best_station} ({best_practices['no_cycle_rate']['value']:.2f}%) to {rec['station_code']} ({rec['current_value']:.2f}%)"
                }
                recommendations['knowledge_sharing'].append(knowledge_rec)
    
    # Add recommendations based on anomalies
    for anomaly in anomalies:
        if anomaly['type'] == 'negative':
            if anomaly['metric'] == 'no_cycle_rate':
                rec = {
                    'station_code': anomaly['station_code'],
                    'date': anomaly['date'],
                    'metric': anomaly['metric'],
                    'current_value': anomaly['value'],
                    'target_value': mean_no_cycle,
                    'potential_impact': 'high',
                    'recommendation': f"Investigate abnormal no-cycle rate of {anomaly['value']:.2f}% on {anomaly['date']}"
                }
                recommendations['high_priority'].append(rec)
            elif anomaly['metric'] == 'planning_rate':
                rec = {
                    'station_code': anomaly['station_code'],
                    'date': anomaly['date'],
                    'metric': anomaly['metric'],
                    'current_value': anomaly['value'],
                    'target_value': mean_planning,
                    'potential_impact': 'high',
                    'recommendation': f"Investigate abnormal planning rate of {anomaly['value']:.2f}% on {anomaly['date']}"
                }
                recommendations['high_priority'].append(rec)
        elif anomaly['type'] == 'unexpected' and anomaly['metric'] == 'correlation':
            rec = {
                'station_code': anomaly['station_code'],
                'date': anomaly['date'],
                'metric': 'correlation',
                'potential_impact': 'medium',
                'recommendation': f"Investigate unexpected correlation pattern: {anomaly['description']}"
            }
            recommendations['medium_priority'].append(rec)
    
    return recommendations

def generate_recommendation_report(df, recommendations, output_dir):
    """
    Generate a markdown report with recommendations.
    
    Args:
        df: DataFrame with station metrics
        recommendations: Dictionary with recommendations
        output_dir: Directory to save the report
    
    Returns:
        Path to the generated report
    """
    # Create report content
    report = "# Container Planning Improvement Recommendations\n\n"
    report += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    # Add executive summary
    report += "## Executive Summary\n\n"
    
    high_count = len(recommendations['high_priority'])
    med_count = len(recommendations['medium_priority'])
    low_count = len(recommendations['low_priority'])
    best_count = len(recommendations['best_practices'])
    
    report += f"This report contains {high_count + med_count + low_count} improvement recommendations "
    report += f"across {len(df['station_code'].unique())} stations, prioritized as follows:\n\n"
    report += f"- **{high_count}** high priority recommendations\n"
    report += f"- **{med_count}** medium priority recommendations\n"
    report += f"- **{low_count}** low priority recommendations\n\n"
    
    report += f"Additionally, **{best_count}** stations have been identified as best practice examples "
    report += f"and **{len(recommendations['knowledge_sharing'])}** knowledge sharing opportunities have been identified.\n\n"
    
    # Add high priority recommendations
    report += "## High Priority Recommendations\n\n"
    if recommendations['high_priority']:
        for i, rec in enumerate(recommendations['high_priority']):
            report += f"### {i+1}. {rec['station_code']}: {rec['recommendation']}\n\n"
            report += f"- **Current Value**: {rec['current_value']:.2f}%\n"
            report += f"- **Target Value**: {rec['target_value']:.2f}%\n"
            report += f"- **Potential Impact**: {rec['potential_impact'].upper()}\n"
            if 'context' in rec:
                report += f"- **Context**: {rec['context']}\n"
            report += "\n"
    else:
        report += "No high priority recommendations at this time.\n\n"
    
    # Add medium priority recommendations
    report += "## Medium Priority Recommendations\n\n"
    if recommendations['medium_priority']:
        for i, rec in enumerate(recommendations['medium_priority']):
            report += f"### {i+1}. {rec['station_code']}: {rec['recommendation']}\n\n"
            if 'current_value' in rec:
                report += f"- **Current Value**: {rec['current_value']:.2f}%\n"
                report += f"- **Target Value**: {rec['target_value']:.2f}%\n"
            report += f"- **Potential Impact**: {rec['potential_impact'].upper()}\n\n"
    else:
        report += "No medium priority recommendations at this time.\n\n"
    
    # Add knowledge sharing opportunities
    report += "## Knowledge Sharing Opportunities\n\n"
    if recommendations['knowledge_sharing']:
        for i, rec in enumerate(recommendations['knowledge_sharing']):
            report += f"### {i+1}. {rec['source_station']} → {rec['target_station']}\n\n"
            report += f"- **Recommendation**: {rec['recommendation']}\n"
            report += f"- **Metric**: {rec['metric']}\n\n"
    else:
        report += "No knowledge sharing opportunities identified at this time.\n\n"
    
    # Add best practice examples
    report += "## Best Practice Examples\n\n"
    if recommendations['best_practices']:
        for i, rec in enumerate(recommendations['best_practices']):
            report += f"### {i+1}. {rec['station_code']}\n\n"
            report += f"- **Planning Rate**: {rec['planning_rate']:.2f}%\n"
            report += f"- **No Cycle Rate**: {rec['no_cycle_rate']:.2f}%\n"
            report += f"- **Recommendation**: {rec['recommendation']}\n\n"
    else:
        report += "No best practice examples identified at this time.\n\n"
    
    # Add low priority recommendations (in appendix)
    report += "## Appendix: Low Priority Recommendations\n\n"
    if recommendations['low_priority']:
        for i, rec in enumerate(recommendations['low_priority']):
            report += f"{i+1}. {rec['station_code']}: {rec['recommendation']}\n"
    else:
        report += "No low priority recommendations at this time.\n\n"
    
    # Save the report
    report_path = os.path.join(output_dir, 'improvement_recommendations.md')
    with open(report_path, 'w') as f:
        f.write(report)
    
    return report_path

def generate_visualization(df, recommendations, output_dir):
    """
    Generate visualizations for the recommendations.
    
    Args:
        df: DataFrame with station metrics
        recommendations: Dictionary with recommendations
        output_dir: Directory to save visualizations
    """
    # Create a visualization showing station performance quadrants
    plt.figure(figsize=(12, 10))
    
    # Get latest metrics for each station
    latest_df = df.sort_values('date').groupby('station_code').last().reset_index()
    
    # Create scatter plot
    sns.scatterplot(
        x='planning_rate', 
        y='no_cycle_rate', 
        size='total_packages',
        hue='station_code',
        data=latest_df,
        sizes=(50, 500),
        alpha=0.7
    )
    
    # Add quadrant lines
    plt.axvline(x=THRESHOLDS['planning_rate']['average'], color='gray', linestyle='--')
    plt.axhline(y=THRESHOLDS['no_cycle_rate']['average'], color='gray', linestyle='--')
    
    # Add quadrant labels
    plt.text(
        THRESHOLDS['planning_rate']['average'] + 5, 
        THRESHOLDS['no_cycle_rate']['average'] + 5, 
        "High Planning,\nHigh No-Cycle\n(Unexpected)",
        fontsize=10, ha='left', va='bottom'
    )
    plt.text(
        THRESHOLDS['planning_rate']['average'] - 5, 
        THRESHOLDS['no_cycle_rate']['average'] + 5, 
        "Low Planning,\nHigh No-Cycle\n(Problematic)",
        fontsize=10, ha='right', va='bottom'
    )
    plt.text(
        THRESHOLDS['planning_rate']['average'] - 5, 
        THRESHOLDS['no_cycle_rate']['average'] - 5, 
        "Low Planning,\nLow No-Cycle\n(Unexpected)",
        fontsize=10, ha='right', va='top'
    )
    plt.text(
        THRESHOLDS['planning_rate']['average'] + 5, 
        THRESHOLDS['no_cycle_rate']['average'] - 5, 
        "High Planning,\nLow No-Cycle\n(Optimal)",
        fontsize=10, ha='left', va='top'
    )
    
    # Add priority annotations
    for rec in recommendations['high_priority']:
        station = rec['station_code']
        station_data = latest_df[latest_df['station_code'] == station]
        
        if not station_data.empty:
            plt.annotate(
                "High Priority",
                xy=(station_data['planning_rate'].values[0], station_data['no_cycle_rate'].values[0]),
                xytext=(10, 10),
                textcoords='offset points',
                fontsize=8,
                color='red',
                arrowprops=dict(arrowstyle='->', color='red')
            )
    
    # Add best practice annotations
    for rec in recommendations['best_practices']:
        station = rec['station_code']
        station_data = latest_df[latest_df['station_code'] == station]
        
        if not station_data.empty:
            plt.annotate(
                "Best Practice",
                xy=(station_data['planning_rate'].values[0], station_data['no_cycle_rate'].values[0]),
                xytext=(10, -10),
                textcoords='offset points',
                fontsize=8,
                color='green',
                arrowprops=dict(arrowstyle='->', color='green')
            )
    
    # Set limits and labels
    plt.xlim(30, 100)
    plt.ylim(0, 50)
    plt.title('Station Performance Quadrant Analysis')
    plt.xlabel('Planning Rate (%)')
    plt.ylabel('No Cycle Rate (%)')
    plt.grid(True, alpha=0.3)
    
    # Save the visualization
    viz_path = os.path.join(output_dir, 'station_performance_quadrants.png')
    plt.savefig(viz_path)
    plt.close()
    
    return viz_path

def export_recommendations_json(recommendations, output_dir):
    """
    Export recommendations in JSON format for programmatic use.
    
    Args:
        recommendations: Dictionary with recommendations
        output_dir: Directory to save the JSON file
    
    Returns:
        Path to the JSON file
    """
    json_path = os.path.join(output_dir, 'recommendations.json')
    
    with open(json_path, 'w') as f:
        json.dump(recommendations, f, indent=2)
    
    return json_path

def main():
    """Main function to run the recommendation engine."""
    print("Starting Container Planning Recommendation Engine...")
    
    # Load station data
    df = load_station_data(RESULTS_DIR)
    print(f"Loaded data for {len(df['station_code'].unique())} stations and {len(df['date'].unique())} dates")
    
    # Analyze station trends
    trend_df = analyze_station_trends(df)
    print(f"Analyzed trends for {len(trend_df)} stations")
    
    # Identify best practices
    best_practices = identify_best_practices(df)
    print(f"Identified {len(best_practices)} best practice examples")
    
    # Detect anomalies
    anomalies = detect_anomalies(df)
    print(f"Detected {len(anomalies)} anomalies")
    
    # Generate recommendations
    recommendations = generate_recommendations(df, trend_df, best_practices, anomalies)
    print(f"Generated recommendations: {len(recommendations['high_priority'])} high priority, "
          f"{len(recommendations['medium_priority'])} medium priority, "
          f"{len(recommendations['low_priority'])} low priority")
    
    # Generate recommendation report
    report_path = generate_recommendation_report(df, recommendations, OUTPUT_DIR)
    print(f"Generated recommendation report: {report_path}")
    
    # Generate visualization
    viz_path = generate_visualization(df, recommendations, OUTPUT_DIR)
    print(f"Generated visualization: {viz_path}")
    
    # Export recommendations as JSON
    json_path = export_recommendations_json(recommendations, OUTPUT_DIR)
    print(f"Exported recommendations to JSON: {json_path}")
    
    print("Recommendation engine complete.")
    
if __name__ == "__main__":
    main()