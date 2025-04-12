import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# Load CSV and clean column names
df = pd.read_csv(
    "path/to/your/csvfile.csv",  # Replace with your CSV file path
    sep=None,  # Auto-detect separator
    engine="python"
)

# Strip spaces & lowercase column names
df.columns = df.columns.str.strip().str.lower()

# Parse timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create subplots (2 rows, 3 columns)
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "Message Throughput (msg/s)",
        "Byte Throughput (MB/s)",
        "Average Latency (ms)",
        "CPU Usage (%)",
        "Memory Usage (MB)",
        "Messages Count"
    ),
    shared_xaxes=True,
    vertical_spacing=0.08
)

# 1. Message Throughput
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['msg_throughput'], mode='lines', name='msg_throughput'), row=1, col=1)

# 2. MB Throughput
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['mb_throughput'], mode='lines', name='mb_throughput', line=dict(color='orange')), row=1, col=2)

# 3. Average Latency
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['avg_latency_ms'], mode='lines', name='avg_latency_ms', line=dict(color='green')), row=2, col=1)

# 4. CPU Usage
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['cpu_usage_percent'], mode='lines', name='cpu_usage_percent', line=dict(color='red')), row=2, col=2)

# 5. Memory Usage
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['mem_usage_mb'], mode='lines', name='mem_usage_mb', line=dict(color='purple')), row=3, col=1)

# 6. Messages Count
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['messages_count'], mode='lines', name='messages_count', line=dict(color='teal')), row=3, col=2)

# Layout
fig.update_layout(
    height=900,
    title_text="Kafka Experiment Dashboard",
    template="plotly_dark",
    showlegend=False
)

fig.show()
