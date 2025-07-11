#!/usr/bin/env python
"""
Interactive Dashboard for Container Planning Analysis

This script creates an interactive dashboard using Plotly to visualize and analyze
container planning data across multiple stations and dates.
"""

import os
import sys
import pandas as pd
import numpy as np
import json
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import warnings
warnings.filterwarnings('ignore')

# Define paths and constants
RESULTS_DIR = '/home/admsia/shipment_timeline/container_planning/analysis/results'
OUTPUT_DIR = '/home/admsia/shipment_timeline/container_planning/analysis/dashboard'
DASHBOARD_PORT = 8050
STATION_COLORS = {
    'DAU1': '#1f77b4',  # blue
    'DAU2': '#ff7f0e',  # orange
    'DAU5': '#2ca02c',  # green
    'DAU7': '#d62728',  # red
    'DJT6': '#9467bd',  # purple
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_analysis_results(results_dir):
    """
    Load analysis results from the results directory.
    
    Args:
        results_dir: Directory containing analysis results
        
    Returns:
        DataFrame containing combined analysis results
    """
    # Try to load results from multi_station_analysis.py output
    report_path = os.path.join(results_dir, 'multi_station_analysis_report.md')
    if os.path.exists(report_path):
        print(f"Found analysis report at {report_path}")
        # If report exists, extract data from it
        return extract_data_from_report(report_path)
    
    # If no report is found, use sample data
    print("No analysis report found. Using sample data.")
    return generate_sample_data()

def extract_data_from_report(report_path):
    """
    Extract analysis data from the markdown report.
    
    Args:
        report_path: Path to the markdown report
        
    Returns:
        DataFrame containing analysis data
    """
    try:
        with open(report_path, 'r') as f:
            content = f.read()
            
        # Extract metrics table
        table_start = content.find("| Station | Date | Total Packages |")
        if table_start == -1:
            print("Could not find metrics table in report")
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
        
        # Create DataFrame from parsed data
        df = pd.DataFrame(data)
        
        # Add calculated columns
        df['planned_packages'] = (df['total_packages'] * df['planning_rate'] / 100).round().astype(int)
        df['inducted_packages'] = df['total_packages'] # Assuming all packages are inducted
        df['no_cycle_packages'] = (df['inducted_packages'] * df['no_cycle_rate'] / 100).round().astype(int)
        df['inducted_as_planned'] = df['inducted_packages'] - df['no_cycle_packages']
        
        return df
    except Exception as e:
        print(f"Error extracting data from report: {e}")
        return generate_sample_data()

def generate_sample_data():
    """
    Generate sample data for dashboard demonstration.
    
    Returns:
        DataFrame containing sample analysis data
    """
    # Sample stations and dates
    stations = ['DAU1', 'DAU2', 'DAU5', 'DAU7', 'DJT6']
    dates = ['2025-06-01', '2025-06-02', '2025-06-03']
    
    # Base data for different stations
    station_profiles = {
        'DAU1': {'planning_base': 95, 'no_cycle_base': 5, 'volume_base': 5000, 'nodes_base': 6},
        'DAU2': {'planning_base': 85, 'no_cycle_base': 15, 'volume_base': 4500, 'nodes_base': 8},
        'DAU5': {'planning_base': 75, 'no_cycle_base': 20, 'volume_base': 3800, 'nodes_base': 10},
        'DAU7': {'planning_base': 80, 'no_cycle_base': 18, 'volume_base': 4200, 'nodes_base': 7},
        'DJT6': {'planning_base': 60, 'no_cycle_base': 35, 'volume_base': 6000, 'nodes_base': 13}
    }
    
    # Generate data with variations
    data = []
    for station in stations:
        profile = station_profiles[station]
        for date in dates:
            # Add random variations
            planning_rate = profile['planning_base'] + np.random.uniform(-5, 5)
            no_cycle_rate = profile['no_cycle_base'] + np.random.uniform(-3, 3)
            total_packages = int(profile['volume_base'] * np.random.uniform(0.9, 1.1))
            originating_nodes = int(profile['nodes_base'] * np.random.uniform(0.9, 1.1))
            
            # Calculate derived metrics
            inducted_packages = total_packages
            planned_packages = int(total_packages * planning_rate / 100)
            no_cycle_packages = int(inducted_packages * no_cycle_rate / 100)
            inducted_as_planned = inducted_packages - no_cycle_packages
            
            data.append({
                'station_code': station,
                'date': date,
                'total_packages': total_packages,
                'planning_rate': planning_rate,
                'no_cycle_rate': no_cycle_rate,
                'originating_nodes': originating_nodes,
                'planned_packages': planned_packages,
                'inducted_packages': inducted_packages,
                'no_cycle_packages': no_cycle_packages,
                'inducted_as_planned': inducted_as_planned
            })
    
    return pd.DataFrame(data)

def create_dashboard(df):
    """
    Create the interactive dashboard app.
    
    Args:
        df: DataFrame containing analysis data
        
    Returns:
        Dash app instance
    """
    # Initialize the Dash app
    app = dash.Dash(
        __name__, 
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        title="Container Planning Dashboard"
    )
    
    # Define station options for dropdown
    station_options = [{'label': station, 'value': station} for station in sorted(df['station_code'].unique())]
    date_options = [{'label': date, 'value': date} for date in sorted(df['date'].unique())]
    
    # Create the app layout
    app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Container Planning Dashboard", className="text-center my-4"),
                html.P("Interactive analysis of planning effectiveness across stations", className="text-center mb-5")
            ])
        ]),
        
        # Filters row
        dbc.Row([
            dbc.Col([
                html.Label("Select Station(s):"),
                dcc.Dropdown(
                    id='station-selector',
                    options=station_options,
                    multi=True,
                    value=df['station_code'].unique().tolist()
                )
            ], width=5),
            
            dbc.Col([
                html.Label("Select Date(s):"),
                dcc.Dropdown(
                    id='date-selector',
                    options=date_options,
                    multi=True,
                    value=df['date'].unique().tolist()
                )
            ], width=5),
            
            dbc.Col([
                html.Label("Metric:"),
                dcc.RadioItems(
                    id='metric-selector',
                    options=[
                        {'label': 'Planning Rate', 'value': 'planning_rate'},
                        {'label': 'No Cycle Rate', 'value': 'no_cycle_rate'}
                    ],
                    value='no_cycle_rate'
                )
            ], width=2)
        ], className="mb-4"),
        
        # KPI cards row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Total Packages", className="card-title"),
                        html.H3(id="kpi-total-packages", className="card-text text-primary")
                    ])
                ])
            ]),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Planning Rate", className="card-title"),
                        html.H3(id="kpi-planning-rate", className="card-text text-success")
                    ])
                ])
            ]),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("No Cycle Rate", className="card-title"),
                        html.H3(id="kpi-no-cycle-rate", className="card-text text-danger")
                    ])
                ])
            ]),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Orig. Nodes", className="card-title"),
                        html.H3(id="kpi-orig-nodes", className="card-text text-info")
                    ])
                ])
            ])
        ], className="mb-4"),
        
        # Main charts row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Planning Effectiveness by Station"),
                    dbc.CardBody([
                        dcc.Graph(id='planning-effectiveness-chart')
                    ])
                ])
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("No Cycle Rate vs Planning Rate"),
                    dbc.CardBody([
                        dcc.Graph(id='correlation-chart')
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        # Additional charts row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Trend Analysis"),
                    dbc.CardBody([
                        dcc.Graph(id='trend-chart')
                    ])
                ])
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Volume Distribution"),
                    dbc.CardBody([
                        dcc.Graph(id='volume-chart')
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        # Recommendations row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Recommendations"),
                    dbc.CardBody([
                        html.Div(id='recommendations')
                    ])
                ])
            ])
        ], className="mb-4"),
        
        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P("Container Planning Analysis Dashboard | Generated on " + pd.Timestamp.now().strftime("%Y-%m-%d"),
                       className="text-center text-muted")
            ])
        ])
    ], fluid=True)
    
    # Define callbacks
    @app.callback(
        [Output('kpi-total-packages', 'children'),
         Output('kpi-planning-rate', 'children'),
         Output('kpi-no-cycle-rate', 'children'),
         Output('kpi-orig-nodes', 'children')],
        [Input('station-selector', 'value'),
         Input('date-selector', 'value')]
    )
    def update_kpis(selected_stations, selected_dates):
        """Update KPI cards based on selection."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        total_packages = filtered_df['total_packages'].sum()
        
        # Calculate weighted averages
        planning_rate = (
            (filtered_df['planning_rate'] * filtered_df['total_packages']).sum() / 
            filtered_df['total_packages'].sum()
        ) if filtered_df['total_packages'].sum() > 0 else 0
        
        no_cycle_rate = (
            (filtered_df['no_cycle_rate'] * filtered_df['inducted_packages']).sum() / 
            filtered_df['inducted_packages'].sum()
        ) if filtered_df['inducted_packages'].sum() > 0 else 0
        
        orig_nodes = filtered_df['originating_nodes'].mean()
        
        return [
            f"{total_packages:,}",
            f"{planning_rate:.2f}%",
            f"{no_cycle_rate:.2f}%",
            f"{orig_nodes:.1f}"
        ]
    
    @app.callback(
        Output('planning-effectiveness-chart', 'figure'),
        [Input('station-selector', 'value'),
         Input('date-selector', 'value'),
         Input('metric-selector', 'value')]
    )
    def update_effectiveness_chart(selected_stations, selected_dates, selected_metric):
        """Update planning effectiveness chart based on selection."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        if selected_metric == 'planning_rate':
            y_col = 'planning_rate'
            title = 'Planning Rate by Station'
            y_title = 'Planning Rate (%)'
        else:
            y_col = 'no_cycle_rate'
            title = 'No Cycle Rate by Station'
            y_title = 'No Cycle Rate (%)'
        
        fig = px.bar(
            filtered_df, 
            x='station_code', 
            y=y_col, 
            color='station_code',
            text=y_col,
            barmode='group',
            facet_col='date',
            facet_col_wrap=3,
            height=400,
            color_discrete_map=STATION_COLORS,
            category_orders={"date": sorted(selected_dates)}
        )
        
        fig.update_layout(
            title=title,
            yaxis_title=y_title,
            xaxis_title='Station',
            legend_title='Station',
            showlegend=True
        )
        
        # Format the text labels
        fig.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
        
        return fig
    
    @app.callback(
        Output('correlation-chart', 'figure'),
        [Input('station-selector', 'value'),
         Input('date-selector', 'value')]
    )
    def update_correlation_chart(selected_stations, selected_dates):
        """Update correlation chart based on selection."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        fig = px.scatter(
            filtered_df,
            x='planning_rate',
            y='no_cycle_rate',
            color='station_code',
            size='total_packages',
            hover_name='station_code',
            hover_data={
                'date': True,
                'planning_rate': ':.1f',
                'no_cycle_rate': ':.1f',
                'total_packages': True,
                'originating_nodes': True
            },
            color_discrete_map=STATION_COLORS,
            height=400,
            labels={
                'planning_rate': 'Planning Rate (%)',
                'no_cycle_rate': 'No Cycle Rate (%)',
                'total_packages': 'Total Packages'
            }
        )
        
        fig.update_layout(
            title='No Cycle Rate vs Planning Rate',
            xaxis_title='Planning Rate (%)',
            yaxis_title='No Cycle Rate (%)',
            legend_title='Station',
            showlegend=True
        )
        
        # Add a trendline
        if len(filtered_df) > 1:
            try:
                x = filtered_df['planning_rate']
                y = filtered_df['no_cycle_rate']
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)
                
                x_range = np.linspace(min(x), max(x), 100)
                
                fig.add_trace(
                    go.Scatter(
                        x=x_range,
                        y=p(x_range),
                        mode='lines',
                        name='Trend',
                        line=dict(color='rgba(0,0,0,0.5)', dash='dash')
                    )
                )
                
                # Calculate and display correlation
                corr = x.corr(y)
                fig.add_annotation(
                    x=min(x) + 0.1 * (max(x) - min(x)),
                    y=max(y) - 0.1 * (max(y) - min(y)),
                    text=f"Correlation: {corr:.2f}",
                    showarrow=False,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="black",
                    borderwidth=1
                )
            except:
                pass
        
        return fig
    
    @app.callback(
        Output('trend-chart', 'figure'),
        [Input('station-selector', 'value'),
         Input('date-selector', 'value'),
         Input('metric-selector', 'value')]
    )
    def update_trend_chart(selected_stations, selected_dates, selected_metric):
        """Update trend chart based on selection."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        if len(selected_dates) <= 1:
            # Not enough dates for trend
            fig = go.Figure()
            fig.add_annotation(
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                text="Trend analysis requires multiple dates",
                showarrow=False,
                font=dict(size=14)
            )
            return fig
        
        if selected_metric == 'planning_rate':
            y_col = 'planning_rate'
            title = 'Planning Rate Trend'
            y_title = 'Planning Rate (%)'
        else:
            y_col = 'no_cycle_rate'
            title = 'No Cycle Rate Trend'
            y_title = 'No Cycle Rate (%)'
        
        # Sort data by date
        filtered_df['date_dt'] = pd.to_datetime(filtered_df['date'])
        filtered_df = filtered_df.sort_values('date_dt')
        
        fig = px.line(
            filtered_df,
            x='date',
            y=y_col,
            color='station_code',
            markers=True,
            height=400,
            color_discrete_map=STATION_COLORS,
            category_orders={"date": sorted(selected_dates)}
        )
        
        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title=y_title,
            legend_title='Station',
            showlegend=True
        )
        
        return fig
    
    @app.callback(
        Output('volume-chart', 'figure'),
        [Input('station-selector', 'value'),
         Input('date-selector', 'value')]
    )
    def update_volume_chart(selected_stations, selected_dates):
        """Update volume chart based on selection."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        # Create a stacked bar chart of package volumes
        volume_data = []
        for station in selected_stations:
            for date in selected_dates:
                station_date_df = filtered_df[(filtered_df['station_code'] == station) & 
                                            (filtered_df['date'] == date)]
                
                if not station_date_df.empty:
                    row = station_date_df.iloc[0]
                    
                    # Add inducted as planned packages
                    volume_data.append({
                        'station_code': station,
                        'date': date,
                        'category': 'Inducted as Planned',
                        'packages': row['inducted_as_planned']
                    })
                    
                    # Add no cycle packages
                    volume_data.append({
                        'station_code': station,
                        'date': date,
                        'category': 'No Cycle',
                        'packages': row['no_cycle_packages']
                    })
                    
                    # Add planned but not inducted packages
                    planned_not_inducted = row['planned_packages'] - row['inducted_as_planned']
                    if planned_not_inducted > 0:
                        volume_data.append({
                            'station_code': station,
                            'date': date,
                            'category': 'Planned Not Inducted',
                            'packages': planned_not_inducted
                        })
        
        volume_df = pd.DataFrame(volume_data)
        
        fig = px.bar(
            volume_df,
            x='station_code',
            y='packages',
            color='category',
            facet_col='date',
            facet_col_wrap=3,
            height=400,
            category_orders={
                "category": ["Inducted as Planned", "No Cycle", "Planned Not Inducted"],
                "date": sorted(selected_dates)
            },
            color_discrete_map={
                "Inducted as Planned": "#2ca02c",  # green
                "No Cycle": "#d62728",            # red
                "Planned Not Inducted": "#ff7f0e"  # orange
            }
        )
        
        fig.update_layout(
            title='Package Volume by Planning Status',
            yaxis_title='Number of Packages',
            xaxis_title='Station',
            legend_title='Status',
            showlegend=True
        )
        
        return fig
    
    @app.callback(
        Output('recommendations', 'children'),
        [Input('station-selector', 'value'),
         Input('date-selector', 'value')]
    )
    def update_recommendations(selected_stations, selected_dates):
        """Generate recommendations based on data analysis."""
        filtered_df = df[(df['station_code'].isin(selected_stations)) & 
                         (df['date'].isin(selected_dates))]
        
        # Get latest data for each station
        latest_df = filtered_df.sort_values('date').groupby('station_code').last().reset_index()
        
        # Generate recommendations
        recommendations = []
        
        # Add overall recommendations
        recommendations.append(html.H5("Overall Recommendations"))
        
        # Identify best and worst performing stations
        if len(latest_df) > 1:
            best_station = latest_df.loc[latest_df['no_cycle_rate'].idxmin()]
            worst_station = latest_df.loc[latest_df['no_cycle_rate'].idxmax()]
            
            recommendations.append(html.P([
                "Best performing station is ",
                html.Strong(best_station['station_code']),
                f" with a no-cycle rate of {best_station['no_cycle_rate']:.2f}%. ",
                "Worst performing station is ",
                html.Strong(worst_station['station_code']),
                f" with a no-cycle rate of {worst_station['no_cycle_rate']:.2f}%."
            ]))
            
            # Knowledge sharing recommendation
            recommendations.append(html.P([
                "Recommend knowledge sharing from ",
                html.Strong(best_station['station_code']),
                " to improve processes at ",
                html.Strong(worst_station['station_code']),
                "."
            ]))
        
        # Add station-specific recommendations
        if selected_stations:
            recommendations.append(html.H5("Station-Specific Recommendations"))
            
            for _, row in latest_df.iterrows():
                station = row['station_code']
                no_cycle = row['no_cycle_rate']
                planning = row['planning_rate']
                nodes = row['originating_nodes']
                
                station_recs = []
                
                if no_cycle > 20:
                    priority = html.Strong("High Priority: ", style={'color': 'red'})
                elif no_cycle > 10:
                    priority = html.Strong("Medium Priority: ", style={'color': 'orange'})
                else:
                    priority = html.Strong("Low Priority: ", style={'color': 'green'})
                
                station_recs.append(html.Li([
                    priority,
                    f"Current no-cycle rate is {no_cycle:.2f}%"
                ]))
                
                if planning < 80:
                    station_recs.append(html.Li([
                        "Improve planning coverage from current ",
                        html.Strong(f"{planning:.2f}%"),
                        " to target of 95%+"
                    ]))
                
                if nodes > 10:
                    station_recs.append(html.Li([
                        "Analyze impact of high originating node count (",
                        html.Strong(f"{nodes}"),
                        ") on planning effectiveness"
                    ]))
                
                recommendations.append(html.Div([
                    html.H6(f"Station {station}"),
                    html.Ul(station_recs)
                ]))
        
        return recommendations

    return app

def save_dashboard_html(app, output_dir):
    """
    Generate a static HTML version of the dashboard.
    
    Args:
        app: The Dash app instance
        output_dir: Directory to save the HTML file
    """
    try:
        # This is a simplified approach since we can't actually render the full interactive dashboard as static HTML
        # Instead, we'll create a basic HTML page with links to the dashboard
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Container Planning Dashboard</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
                h1 { color: #333; }
                .card { border: 1px solid #ddd; border-radius: 8px; padding: 20px; margin-bottom: 20px; }
                .instruction { background-color: #f8f9fa; padding: 15px; border-radius: 8px; }
            </style>
        </head>
        <body>
            <h1>Container Planning Analysis Dashboard</h1>
            
            <div class="card">
                <h2>Interactive Dashboard</h2>
                <p>The interactive dashboard is available when running the following command:</p>
                <div class="instruction">
                    <code>python /home/admsia/shipment_timeline/container_planning/analysis/interactive_dashboard.py</code>
                </div>
                <p>This will start a local web server at <a href="http://localhost:8050">http://localhost:8050</a></p>
            </div>
            
            <div class="card">
                <h2>Dashboard Features</h2>
                <ul>
                    <li>Interactive station and date filtering</li>
                    <li>Planning effectiveness visualization</li>
                    <li>Correlation analysis between planning rate and no cycle rate</li>
                    <li>Trend analysis over time</li>
                    <li>Volume distribution by planning status</li>
                    <li>Data-driven recommendations</li>
                </ul>
            </div>
            
            <div class="card">
                <h2>Generated Reports</h2>
                <p>The dashboard uses data from the multi-station analysis. If you haven't run the analysis yet, use:</p>
                <div class="instruction">
                    <code>python /home/admsia/shipment_timeline/container_planning/analysis/multi_station_analysis.py</code>
                </div>
            </div>
            
            <footer>
                <hr>
                <p>Container Planning Analysis Dashboard | Generated on %s</p>
            </footer>
        </body>
        </html>
        """ % pd.Timestamp.now().strftime("%Y-%m-%d")
        
        html_path = os.path.join(output_dir, 'dashboard.html')
        with open(html_path, 'w') as f:
            f.write(html_content)
            
        print(f"Static HTML dashboard info saved to {html_path}")
    except Exception as e:
        print(f"Error saving dashboard HTML: {e}")

def main():
    """Main function to run the dashboard."""
    print("Starting Container Planning Dashboard...")
    
    # Load analysis results
    df = load_analysis_results(RESULTS_DIR)
    print(f"Loaded data for {len(df['station_code'].unique())} stations and {len(df['date'].unique())} dates")
    
    # Create the dashboard app
    app = create_dashboard(df)
    
    # Save a static HTML version
    save_dashboard_html(app, OUTPUT_DIR)
    
    # Run the app
    print(f"Starting dashboard server on port {DASHBOARD_PORT}...")
    print(f"Visit http://localhost:{DASHBOARD_PORT} in your browser")
    app.run_server(debug=True, port=DASHBOARD_PORT)
    
if __name__ == "__main__":
    main()