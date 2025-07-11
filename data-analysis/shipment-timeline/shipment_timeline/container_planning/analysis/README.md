# Container Planning Analysis Framework

This framework provides comprehensive tools for analyzing container planning data, identifying patterns, and generating actionable insights to improve planning effectiveness and reduce no-cycle occurrences.

## Overview

The Container Planning Analysis Framework integrates data from multiple sources to provide a complete view of container planning effectiveness across the delivery network. It includes tools for data extraction, analysis, visualization, and recommendation generation.

## Components

### 1. Data Analysis Tools

- **`analyze_parquet.py`**: Base script for analyzing parquet files from S3
- **`multi_station_analysis.py`**: Advanced script for comparing metrics across multiple stations
- **`run_analysis.sh`**: Shell script for downloading and running analysis on sample data

### 2. SQL Integration

- **`integrated_timeline_queries.sql`**: SQL views and functions for combining container planning with shipment timeline data
- **`container_planning_sample_queries.sql`**: Sample queries for common container planning analyses

### 3. Visualization & Dashboard

- **`container_analysis.ipynb`**: Jupyter notebook for exploratory data analysis and visualization
- **`interactive_dashboard.py`**: Interactive Plotly/Dash dashboard for monitoring metrics

### 4. Recommendation Engine

- **`recommendation_engine.py`**: Framework for generating prioritized improvement recommendations based on data patterns

## Directory Structure

```
/container_planning/
├── analysis/
│   ├── README.md                     # This documentation
│   ├── analyze_parquet.py            # Base parquet analysis script
│   ├── multi_station_analysis.py     # Multi-station comparison script
│   ├── run_analysis.sh               # Shell script for running analysis
│   ├── container_analysis.ipynb      # Jupyter notebook for analysis
│   ├── interactive_dashboard.py      # Interactive dashboard
│   ├── recommendation_engine.py      # Recommendation framework
│   ├── integrated_timeline_queries.sql # SQL integration queries
│   ├── dashboard/                    # Dashboard output directory
│   │   └── dashboard_preview.md      # Dashboard documentation
│   ├── recommendations/              # Recommendation output directory
│   └── results/                      # Analysis results directory
└── table_schemas.md                  # Container planning table schemas
```

## Installation & Dependencies

This framework requires:

1. **Python Libraries**:
   - pandas
   - pyarrow
   - numpy
   - matplotlib
   - seaborn
   - plotly
   - dash (for interactive dashboard)

2. **AWS Command Line Tools**:
   - AWS CLI with S3 access

3. **Database Access**:
   - Redshift access for SQL queries

## Usage Guide

### 1. Basic Parquet Analysis

```bash
# Analyze a single parquet file
python analyze_parquet.py /path/to/parquet/file.parquet

# Analyze all files in a directory
python analyze_parquet.py /path/to/parquet/directory
```

### 2. Multi-Station Analysis

```bash
# Run analysis for all configured stations
python multi_station_analysis.py

# Results are saved to the results/ directory
```

### 3. Interactive Dashboard

```bash
# Launch the interactive dashboard
python interactive_dashboard.py

# Access the dashboard at http://localhost:8050
```

### 4. Recommendation Engine

```bash
# Generate prioritized recommendations
python recommendation_engine.py

# Results are saved to the recommendations/ directory
```

### 5. SQL Integration

The SQL integration queries can be executed in your Redshift environment to create:

1. Integrated timeline views combining container planning with shipment events
2. Package journey tracking across the entire supply chain
3. Planning effectiveness analysis by station and region
4. Timeline gap identification
5. Originating node impact analysis

## Analysis Methodology

### 1. Data Sources

This framework analyzes data from:

- **Container Planning Data**: `amzlcore.amzl_routing2_container_pkg_na` in Redshift
- **Parquet Files**: `s3://altdatasetexfil/claudecloud/routing2_container_snip/`
- **Shipment Timeline**: `amzlcore.d_shipment_timeline` in Redshift
- **Location Mapping**: `amzlanalytics.perfectmile.d_perfectmile_node_mapping_mdm` in Redshift

### 2. Key Metrics

The framework focuses on these key metrics:

- **Planning Rate**: Percentage of packages with plans (`is_planned`)
- **No-Cycle Rate**: Percentage of inducted packages without plans (`is_inducted_not_planned / is_inducted`)
- **Planning Execution Rate**: Percentage of planned packages that were inducted as planned (`is_inducted_as_planned / is_planned`)
- **Process Timing**: Time between planning, induction, and stowing

### 3. Analysis Dimensions

Analysis can be conducted across multiple dimensions:

- **Station**: Individual delivery stations
- **Region**: Geographic regions
- **Date**: Daily, weekly, and monthly time periods
- **Originating Node**: Package sources

### 4. Recommendation Prioritization

Recommendations are prioritized based on:

1. **Impact Potential**: Volume-weighted improvement opportunity
2. **Current Performance**: How far metrics are from targets
3. **Trend Analysis**: Whether metrics are improving or declining
4. **Anomaly Detection**: Identification of unexpected patterns

## Best Practices

1. **Always Use Location Mapping**: Ensure proper timezone handling with location mapping
2. **Filter by Minimum Volume**: Apply volume thresholds to prevent skewed results
3. **Use Time-Based Aggregations**: Analyze patterns by day, week, and month
4. **Calculate Volatility Metrics**: Use standard deviation to understand metric stability
5. **Create Base Tables First**: Use temporary tables with base calculations for efficient analysis
6. **Compare Across Dimensions**: Analyze by station, region, originating node, and time periods

## Known Limitations

1. The framework assumes all Redshift tables and S3 buckets are accessible with current credentials
2. Dashboard performance may degrade with very large datasets
3. Integration queries assume specific schema structure in the timeline data
4. Visualization tools require X server or appropriate environment variables for rendering

## Troubleshooting

### Common Issues

1. **Missing Data**:
   - Check S3 bucket permissions
   - Verify date/station parameters match available data

2. **SQL Integration Errors**:
   - Verify Redshift connection
   - Check table/column names match your environment

3. **Dashboard Not Loading**:
   - Check required Python packages are installed
   - Verify port 8050 is available

## Contributing

To extend this framework:

1. Add new metrics to the `analyze_parquet.py` script
2. Create additional visualizations in the dashboard
3. Extend SQL queries for deeper integration
4. Enhance the recommendation engine with additional insights

## Contact

For questions or support, contact the Container Planning Analysis team.