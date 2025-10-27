import pandas as pd
import numpy as np
import plotly.graph_objects as go
from tqdm import tqdm

# Read the disease connection matrix
print("Loading disease connection matrix...")
df = pd.read_csv('data/disease_graph/disease_connection_matrix.csv', index_col=0)

# Read the codes mapping file
print("Loading codes mapping...")
codes_df = pd.read_csv('data/disease_graph/codes.tsv', sep='\t')
code_to_name = dict(zip(codes_df['coding'], codes_df['meaning']))

# Get the disease names (index/columns of the matrix)
diseases = df.index.tolist()

# Verify that all diseases in the matrix have a mapping, warn if missing
missing_codes = [code for code in diseases if code not in code_to_name]
if missing_codes:
    print(f"Warning: {len(missing_codes)} codes not found in codes.tsv: {missing_codes[:10]}...")

# Verify matrix content and find top 10 largest values in the lower triangle
print("Analyzing matrix for top 10 largest values...")
matrix = df.to_numpy(dtype=np.float64)  # Ensure float64 for numerical stability
lower_triangle_values = []
for i in range(len(diseases)):
    for j in range(i):  # Only below the main diagonal
        value = matrix[i, j]
        if value > 0:  # Collect non-zero values
            lower_triangle_values.append((value, diseases[i], diseases[j]))

# Sort and get top 10 largest values
lower_triangle_values.sort(reverse=True)
top_10_values = lower_triangle_values[:10]

print("Top 10 largest values in the lower triangle:")
for value, disease1, disease2 in top_10_values:
    name1 = code_to_name.get(disease1, disease1)  # Use code if name not found
    name2 = code_to_name.get(disease2, disease2)
    print(f"{disease1} ({name1}) - {disease2} ({name2}): {value}")

# Create a masked matrix for the lower triangle (excluding diagonal)
print("Processing lower triangle of the matrix...")
weight_threshold = 0  # Include all non-zero values
masked_matrix = np.zeros_like(matrix, dtype=np.float64)
hover_text = np.full_like(matrix, '', dtype=object)  # Initialize with empty strings
non_zero_count = 0
for i in tqdm(range(len(diseases)), desc="Extracting lower triangle"):
    for j in range(i):  # Only below the main diagonal
        value = matrix[i, j]
        if value > weight_threshold:
            masked_matrix[i, j] = value
            name1 = code_to_name.get(diseases[i], diseases[i])  # Fallback to code if name missing
            name2 = code_to_name.get(diseases[j], diseases[j])
            hover_text[i, j] = f"{diseases[j]} ({name2}) - {diseases[i]} ({name1}): {value}"
            non_zero_count += 1

print(f"Non-zero values in lower triangle (threshold > {weight_threshold}): {non_zero_count}")

# Create heatmap
print("Creating interactive heatmap...")
fig = go.Figure(data=go.Heatmap(
    z=masked_matrix,
    x=diseases,
    y=diseases,
    text=hover_text,
    hoverinfo='text',  # Use custom text for hover
    colorscale=[
        [0, 'black'],  # Zero values
        [1e-6, 'black'],  # Ensure zero is black
        [1e-6 + 1e-9, '#440154'],  # Start of Viridis for small non-zero values
        [5000.0 / matrix.max(), '#FDE725'],  # End of Viridis at 5,000
        [5000.0 / matrix.max() + 1e-6, '#FF0000'],  # Bright red for > 5,000
        [1.0, '#FF0000']  # Bright red for max
    ],
    zmin=0,
    zmax=matrix.max(),
    colorbar=dict(
        title=dict(
            text='Co-occurrence Count',
            side='right'
        )
    )
))

# Update layout for better visualization and interactivity
fig.update_layout(
    title="Interactive Disease Connection Heatmap (Lower Triangle, Zeros in Black, >5K in Red)",
    xaxis_title="Disease",
    yaxis_title="Disease",
    xaxis=dict(
        tickangle=45,
        tickfont=dict(size=8),
        side='top',  # Move x-axis labels to top for better visibility
        automargin=True
    ),
    yaxis=dict(
        tickfont=dict(size=8),
        autorange='reversed',  # Reverse y-axis to align with matrix orientation
        automargin=True
    ),
    width=1200,
    height=1200,
    margin=dict(l=150, r=150, t=200, b=150),
    hovermode='closest'
)

# Save the plot as an interactive HTML file
print("Saving interactive heatmap...")
fig.write_html('data/disease_graph/disease_connection_heatmap.html', auto_open=True)
print("Interactive heatmap saved to 'data/disease_graph/disease_connection_heatmap.html'")