import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import os
import numpy as np
from datetime import datetime

# Define all four brokers - we're only using 1KB message size
brokers = ["Kafka", "RabbitMQ", "Pulsar", "Redpanda"]
message_size = "1KB"  # Fixed to 1KB only
standard_duration = 840  # 14 minutes in seconds

# Define file paths for different brokers (1KB only)
file_paths = {
    "Kafka": "/Users/noumanqureshi/Downloads/csc-things/kafka-1024.csv",
    "RabbitMQ": "/Users/noumanqureshi/Downloads/csc-things/rabbitmq1024.csv",
    "Pulsar": "/Users/noumanqureshi/Downloads/csc-things/pulsar1.csv",
    "Redpanda": "/Users/noumanqureshi/Downloads/csc-things/redpanda1.csv"
}

# Colors for each broker
broker_colors = {
    "Kafka": '#FF9500',    # Orange
    "RabbitMQ": '#5794f2', # Blue
    "Pulsar": '#73bf69',   # Green
    "Redpanda": '#f2495c'  # Red
}

# Load data for all brokers and standardize to 14 minutes
print("Loading and standardizing 1KB message size data files...")
all_data = {}
for broker in brokers:
    path = file_paths.get(broker)
    if path and os.path.exists(path):
        try:
            df = pd.read_csv(path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['seconds'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds()
            standardized_df = df[df['seconds'] <= standard_duration].copy()
            if len(standardized_df) > 0:
                all_data[broker] = standardized_df
                print(f"  ✓ Loaded {broker} {message_size} data: {len(standardized_df)} records (standardized to 14 minutes)")
            else:
                print(f"  ✗ Error: {broker} data is shorter than 14 minutes, no data available after filtering")
        except Exception as e:
            print(f"  ✗ Error loading {broker} data: {e}")
    else:
        print(f"  ✗ Missing file for {broker} at {path}")

# Define metrics
metrics_to_compare = [
    ('msg_throughput', 'Message Throughput', 'Messages/sec'),
    ('avg_latency_ms', 'Latency', 'Milliseconds'),
    ('mem_usage_mb', 'Memory Usage', 'Memory (MB)')
]

# CALCULATE AVERAGES FOR COMPARISON BASED ON STANDARDIZED DATA
summary_data = []
for broker in brokers:
    if broker in all_data:
        df = all_data[broker]
        summary_data.append({
            'Broker': broker,
            'Message Size': message_size,
            'Test Duration': '14 minutes',
            'Avg Throughput (msgs/sec)': round(df['msg_throughput'].mean(), 2),
            'Avg Latency (ms)': round(df['avg_latency_ms'].mean(), 2),
            'Avg Memory (MB)': round(df['mem_usage_mb'].mean(), 2),
            'CPU Usage (%)': round(df['cpu_usage_percent'].mean(), 2) if 'cpu_usage_percent' in df.columns else np.nan,
            'Max Throughput (msgs/sec)': round(df['msg_throughput'].max(), 2),
            'Min Latency (ms)': round(df['avg_latency_ms'].min(), 2),
        })
summary_df = pd.DataFrame(summary_data)
print("\nStandardized Performance Summary (14 minutes):")
print(summary_df.to_string(index=False))

# --- [Define functions to create each Plotly figure] ---

# Grafana-like dark theme settings for Plotly
plotly_dark_template = go.layout.Template()
plotly_dark_template.layout = go.Layout(
    plot_bgcolor='#212124',
    paper_bgcolor='#181b1f',
    font=dict(color='#d8d9da'),
    xaxis=dict(gridcolor='#2c3235', zerolinecolor='#2c3235', linecolor='#2c3235'),
    yaxis=dict(gridcolor='#2c3235', zerolinecolor='#2c3235', linecolor='#2c3235'),
    legend=dict(bgcolor='rgba(0,0,0,0)') # Transparent legend background
)

# 1. LINE CHARTS COMPARING BROKERS (Plotly version)
def create_line_charts():
    fig = make_subplots(
        rows=len(metrics_to_compare), cols=1,
        subplot_titles=[m[1] for m in metrics_to_compare],
        vertical_spacing=0.1
    )
    for i, (metric_key, title, ylabel) in enumerate(metrics_to_compare):
        for broker_name in brokers:
            if broker_name in all_data:
                df = all_data[broker_name]
                fig.add_trace(
                    go.Scatter(
                        x=df['seconds'],
                        y=df[metric_key],
                        name=f"{broker_name}",
                        line=dict(color=broker_colors[broker_name]),
                        legendgroup=f"group{i}", # Group legends per subplot
                        showlegend=True if i == 0 else False # Show legend only for the first subplot to avoid repetition
                    ),
                    row=i+1, col=1
                )
        fig.update_yaxes(title_text=ylabel, row=i+1, col=1)
        fig.update_xaxes(range=[0, standard_duration], row=i+1, col=1)
    fig.update_xaxes(title_text="Time (seconds)", row=len(metrics_to_compare), col=1)
    fig.update_layout(
        title_text='Messaging Broker Comparison - 1KB Message Size (14 Minutes Standardized)',
        height=400 * len(metrics_to_compare), # Adjust height as needed
        template=plotly_dark_template,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

# 2. BAR CHARTS COMPARING STANDARDIZED METRICS (Plotly version)
def create_bar_charts():
    bar_metrics = [
        ('Avg Throughput (msgs/sec)', 'Messages/sec'),
        ('Avg Latency (ms)', 'Milliseconds'),
        ('Avg Memory (MB)', 'Memory (MB)')
    ]
    fig = make_subplots(
        rows=len(bar_metrics), cols=1,
        subplot_titles=[bm[0] for bm in bar_metrics],
        vertical_spacing=0.15
    )
    for i, (metric_col_name, ylabel) in enumerate(bar_metrics):
        broker_names_list = summary_df['Broker'].tolist()
        values = summary_df[metric_col_name].tolist()
        colors = [broker_colors[b] for b in broker_names_list]
        
        fig.add_trace(
            go.Bar(
                x=broker_names_list,
                y=values,
                marker_color=colors,
                text=[f'{v:.1f}' for v in values], # Add text for values on bars
                textposition='auto' # Or 'outside'
            ),
            row=i+1, col=1
        )
        fig.update_yaxes(title_text=ylabel, row=i+1, col=1)
        fig.update_xaxes(title_text="Messaging Broker", row=i+1, col=1)

    fig.update_layout(
        title_text='1KB Message Size: Four Broker Performance Comparison (14 Minutes Standardized) - Averages',
        height=350 * len(bar_metrics),
        template=plotly_dark_template,
        showlegend=False # No legend needed for these bar charts
    )
    return fig

# 3. BOXPLOTS FOR STATISTICAL DISTRIBUTION (Plotly version)
def create_box_plots():
    fig = make_subplots(
        rows=len(metrics_to_compare), cols=1,
        subplot_titles=[m[1] for m in metrics_to_compare],
        vertical_spacing=0.1
    )
    
    # Prepare data for boxplots
    boxplot_data_list = []
    for broker_name in brokers:
        if broker_name in all_data:
            df = all_data[broker_name]
            for _, row_data in df.iterrows():
                boxplot_data_list.append({
                    'Broker': broker_name,
                    'Message Throughput': row_data['msg_throughput'],
                    'Latency': row_data['avg_latency_ms'],
                    'Memory Usage': row_data['mem_usage_mb']
                })
    plotly_boxplot_df = pd.DataFrame(boxplot_data_list)

    for i, (metric_key_original, title, ylabel) in enumerate(metrics_to_compare):
        # Map original metric key to the column name used in plotly_boxplot_df
        metric_col_name_map = {
            'msg_throughput': 'Message Throughput',
            'avg_latency_ms': 'Latency',
            'mem_usage_mb': 'Memory Usage'
        }
        metric_col_name = metric_col_name_map[metric_key_original]

        for broker_name in brokers: # Plot one broker at a time to assign specific colors
            if broker_name in plotly_boxplot_df['Broker'].unique():
                broker_df = plotly_boxplot_df[plotly_boxplot_df['Broker'] == broker_name]
                fig.add_trace(
                    go.Box(
                        y=broker_df[metric_col_name],
                        name=broker_name,
                        marker_color=broker_colors[broker_name],
                        boxmean=True, # Show mean
                        legendgroup=f"group_box{i}",
                        showlegend=True if i == 0 else False
                    ),
                    row=i+1, col=1
                )
        fig.update_yaxes(title_text=ylabel, row=i+1, col=1)
        fig.update_xaxes(title_text="Messaging Broker", row=i+1, col=1) # Applies to all subplots

    fig.update_layout(
        title_text='1KB Message Size: Statistical Distribution of Performance Metrics (14 Minutes Standardized)',
        height=400 * len(metrics_to_compare),
        template=plotly_dark_template,
        boxmode='group', # Group box plots for each metric
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

# 4. DEDICATED MEMORY LINE GRAPH FUNCTION
def create_memory_line_graph():
    """
    Create a dedicated line graph just for memory usage to better show trends
    """
    fig = go.Figure()
    
    # Add a trace for each broker's memory usage
    for broker_name in brokers:
        if broker_name in all_data:
            df = all_data[broker_name]
            
            # Add memory usage line
            fig.add_trace(
                go.Scatter(
                    x=df['seconds'],
                    y=df['mem_usage_mb'],
                    name=f"{broker_name}",
                    line=dict(
                        color=broker_colors[broker_name],
                        width=3  # Make lines thicker for better visibility
                    ),
                    mode='lines'  # Use 'lines+markers' if you want points at each data point
                )
            )
    
    # Optional: Add a trendline for each broker
    for broker_name in brokers:
        if broker_name in all_data:
            df = all_data[broker_name]
            
            # Skip if not enough data points
            if len(df) < 5:
                continue
                
            # Calculate simple trendline (linear regression)
            x = df['seconds']
            y = df['mem_usage_mb']
            
            # Calculate trendline using numpy's polyfit
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            
            # Add trendline as a dashed line
            fig.add_trace(
                go.Scatter(
                    x=x,
                    y=p(x),
                    name=f"{broker_name} Trend",
                    line=dict(
                        color=broker_colors[broker_name],
                        width=2,
                        dash='dash'
                    ),
                    showlegend=False  # Hide from legend to avoid clutter
                )
            )
    
    # Add annotations to show trend direction
    for broker_name in brokers:
        if broker_name in all_data:
            df = all_data[broker_name]
            if len(df) >= 5:
                # Get first and last memory values to determine trend
                first_mem = df.iloc[0]['mem_usage_mb']
                last_mem = df.iloc[-1]['mem_usage_mb']
                
                # Calculate slope
                slope = (last_mem - first_mem) / standard_duration
                
                # Create trend text
                if abs(slope) < 0.0001:  # Very small slope
                    trend_text = "Stable"
                elif slope > 0:
                    trend_text = f"+{slope:.5f} MB/s"
                else:
                    trend_text = f"{slope:.5f} MB/s"
                
                # Add annotation near the end of the line
                fig.add_annotation(
                    x=standard_duration * 0.95,
                    y=last_mem,
                    text=f"{broker_name}: {trend_text}",
                    showarrow=False,
                    xanchor="right",
                    bgcolor="rgba(0,0,0,0.5)",
                    font=dict(color=broker_colors[broker_name])
                )
    
    # Update layout with Grafana-like dark theme
    fig.update_layout(
        title='Memory Usage Over Time (14 Minutes Standardized)',
        xaxis_title='Time (seconds)',
        yaxis_title='Memory Usage (MB)',
        height=600,  # Taller graph for better detail
        template=plotly_dark_template,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    # Set y-axis to start from 0 for better perspective
    fig.update_yaxes(rangemode="tozero")
    
    # Add grid lines for better readability
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2c3235')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2c3235')
    
    return fig

# 5. RADAR CHART FUNCTION
def create_radar_chart():
    radar_metrics_map = {
        'Throughput': 'Avg Throughput (msgs/sec)',
        'Latency (lower is better)': 'Avg Latency (ms)',
        'Memory (lower is better)': 'Avg Memory (MB)',
    }
    radar_data_plotly = {}
    
    # Normalize data (ensure summary_df is available)
    if not summary_df.empty:
        max_throughput = summary_df['Avg Throughput (msgs/sec)'].max()
        max_latency = summary_df['Avg Latency (ms)'].max()
        max_memory = summary_df['Avg Memory (MB)'].max()

        # Handle cases where max values could be zero to avoid division by zero
        max_throughput = max_throughput if max_throughput > 0 else 1
        max_latency = max_latency if max_latency > 0 else 1
        max_memory = max_memory if max_memory > 0 else 1

        for _, row in summary_df.iterrows():
            broker_name = row['Broker']
            throughput_norm = row['Avg Throughput (msgs/sec)'] / max_throughput
            latency_norm = 1 - (row['Avg Latency (ms)'] / max_latency) # Inverted: higher is better
            memory_norm = 1 - (row['Avg Memory (MB)'] / max_memory)   # Inverted: higher is better
            radar_data_plotly[broker_name] = [throughput_norm, latency_norm, memory_norm]

    fig = go.Figure()
    categories = list(radar_metrics_map.keys())

    for broker_name in brokers:
        if broker_name in radar_data_plotly:
            fig.add_trace(go.Scatterpolar(
                r=radar_data_plotly[broker_name],
                theta=categories,
                fill='toself',
                name=broker_name,
                line_color=broker_colors[broker_name]
            ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title='1KB Message Size: Normalized Broker Performance Comparison (14 Minutes Standardized)',
        height=700,
        template=plotly_dark_template,
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
    )
    return fig

# --- [Initialize Dash App] ---
app = dash.Dash(__name__)
app.title = "Messaging Broker Dashboard"

# --- [Define App Layout] ---
# Apply dark theme styles to the main div
app.layout = html.Div(style={'backgroundColor': '#181b1f', 'color': '#d8d9da', 'padding': '20px'}, children=[
    # Use iframe to inject CSS for PDF formatting
    html.Iframe(
        srcDoc='''
            <style>
                @media print {
                    /* Remove page break to keep everything flowing */
                    .no-break {
                        page-break-inside: avoid;
                    }
                    .dash-graph {
                        page-break-inside: avoid;
                    }
                    .dash-table-container {
                        page-break-inside: avoid;
                    }
                    /* Added spacing between sections for visual separation */
                    .section-container {
                        margin-bottom: 40px;
                        padding-bottom: 20px;
                        border-bottom: 1px dashed #2c3235;
                    }
                    /* Last section shouldn't have a border */
                    .section-container:last-child {
                        border-bottom: none;
                    }
                    body {
                        -webkit-print-color-adjust: exact !important;
                        color-adjust: exact !important;
                        print-color-adjust: exact !important;
                    }
                    @page {
                        size: A4 portrait;
                        margin: 1.5cm;
                    }
                }
            </style>
        ''',
        style={'display': 'none'}  # Hide the iframe
    ),

    html.Div(className="header no-break section-container", children=[
        html.H1(
            children='Messaging Broker Performance Dashboard (1KB - 14 Mins Standardized)',
            style={'textAlign': 'center', 'color': '#d8d9da', 'marginBottom': '20px'}
        ),
        
        # Add print instructions
        html.Div(style={'backgroundColor': '#2c3235', 'padding': '10px', 'marginBottom': '20px', 'borderRadius': '4px'}, children=[
            html.H3("PDF Export Instructions", style={'color': '#d8d9da', 'marginTop': '0'}),
            html.Ol([
                html.Li("Use Chrome browser for best results"),
                html.Li("Right-click anywhere and select 'Print...' (or press Cmd+P on Mac/Ctrl+P on Windows)"),
                html.Li("Set Destination to 'Save as PDF'"),
                html.Li("In More Settings, enable 'Background graphics'"),
                html.Li("Set Paper size to 'A4' and Orientation to 'Portrait'"),
                html.Li("Click 'Save' to generate your PDF")
            ], style={'color': '#d8d9da', 'paddingLeft': '20px'})
        ])
    ]),

    html.Div(className="summary-table no-break section-container", children=[
        html.H2("Performance Summary Table", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dash_table.DataTable(
            id='summary-table',
            columns=[{"name": i, "id": i} for i in summary_df.columns],
            data=summary_df.to_dict('records'),
            style_header={
                'backgroundColor': '#2c3235',
                'fontWeight': 'bold',
                'color': '#d8d9da',
                'border': '1px solid #555'
            },
            style_cell={
                'backgroundColor': '#212124',
                'color': '#d8d9da',
                'border': '1px solid #555',
                'padding': '8px',
                'textAlign': 'left'
            },
            style_table={'overflowX': 'auto', 'marginTop': '10px', 'marginBottom': '30px'}
        )
    ]),

    html.Div(className="line-charts no-break section-container", children=[
        html.H2("Time Series: Broker Comparison", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dcc.Graph(
            id='line-charts-comparison',
            figure=create_line_charts()
        )
    ]),

    html.Div(className="memory-line-graph no-break section-container", children=[
        html.H2("Memory Usage Trends", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dcc.Graph(
            id='memory-line-graph',
            figure=create_memory_line_graph()
        )
    ]),

    html.Div(className="bar-charts no-break section-container", children=[
        html.H2("Average Metrics Comparison (Bar Charts)", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dcc.Graph(
            id='bar-charts-comparison',
            figure=create_bar_charts()
        )
    ]),

    html.Div(className="box-plots no-break section-container", children=[
        html.H2("Statistical Distribution (Box Plots)", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dcc.Graph(
            id='box-plots-comparison',
            figure=create_box_plots()
        )
    ]),

    html.Div(className="radar-chart no-break section-container", children=[
        html.H2("Normalized Performance Radar Chart", style={'marginTop': '40px', 'color': '#d8d9da'}),
        dcc.Graph(
            id='radar-chart-comparison',
            figure=create_radar_chart()
        )
    ])
])

# --- [Run the App] ---
if __name__ == '__main__':
    if not all_data:
        print("\nERROR: No data was loaded. Cannot start the dashboard.")
        print("Please check your file paths and data loading logic.")
    else:
        print("\nAll visualizations have been prepared for the Dash app.")
        print(f"Dash app will run on http://127.0.0.1:8050/")
        app.run_server(debug=True)