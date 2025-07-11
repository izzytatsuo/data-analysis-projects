# Container Planning Analysis Dashboard

## Overview

The interactive dashboard provides a comprehensive view of container planning metrics across stations and dates. It enables data-driven decision making through visual analysis of planning effectiveness, no-cycle rates, and correlation patterns.

## Dashboard Features

### 1. Interactive Filtering
- Filter by multiple stations
- Filter by multiple dates
- Select metrics to visualize (planning rate or no-cycle rate)

### 2. KPI Summary Cards
- Total package volume
- Average planning rate
- Average no-cycle rate
- Average originating node count

### 3. Visualizations
- **Planning Effectiveness by Station**: Bar charts showing planning or no-cycle rates by station and date
- **No Cycle Rate vs Planning Rate**: Scatter plot with trend line showing correlation between metrics
- **Trend Analysis**: Line chart tracking metrics over time for selected stations
- **Volume Distribution**: Stacked bar chart showing package volumes by planning status

### 4. Data-Driven Recommendations
- Overall recommendations based on comparative analysis
- Station-specific recommendations with prioritization
- Best practice knowledge sharing suggestions

## Using the Dashboard

To launch the interactive dashboard:

1. Run the dashboard script:
   ```bash
   python /home/admsia/shipment_timeline/container_planning/analysis/interactive_dashboard.py
   ```

2. Open a browser and navigate to:
   ```
   http://localhost:8050
   ```

3. Use the dropdowns and radio buttons to filter and explore the data

## Example Insights

The dashboard is designed to help identify:

1. **Stations with high no-cycle rates** that need process improvements
2. **Correlation patterns** between planning rate and no-cycle rate
3. **Trends over time** showing improvement or degradation in metrics
4. **Volume distribution patterns** highlighting planning execution gaps
5. **Best practice examples** from high-performing stations

## Integration with Analysis Workflow

The dashboard automatically loads results from the multi-station analysis. To refresh the data:

1. Run the multi-station analysis script:
   ```bash
   python /home/admsia/shipment_timeline/container_planning/analysis/multi_station_analysis.py
   ```

2. Launch the dashboard to view the updated results

## Dashboard Architecture

The dashboard is built using:
- **Plotly** for interactive visualizations
- **Dash** for the web application framework
- **Bootstrap** for responsive layout and styling

The dashboard uses a modular design with:
- Data loading component
- Layout definition
- Callback functions for interactivity
- Recommendation engine logic