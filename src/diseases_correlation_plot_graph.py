import pandas as pd
import numpy as np
import plotly.graph_objects as go
from tqdm import tqdm
import os

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# === CONFIGURATION ===
CONNECTION_MATRIX_PATH = 'data/pipelines/z_score_pipeline/poc/ci_analysis/young_upper_ci_analysis.csv'
CODES_PATH = 'data/disease_graph/input/codes.tsv'
OUTPUT_HTML_PATH = "data/disease_graph/output/disease_connection_heatmap_poc.html"

# ------------------------------------------------------------------ #
# Ensure output directory exists
# ------------------------------------------------------------------ #
os.makedirs(os.path.dirname(OUTPUT_HTML_PATH), exist_ok=True)
print(f"Ensured output directory exists: {os.path.dirname(OUTPUT_HTML_PATH)}")

# ------------------------------------------------------------------ #
# 1. Load data
# ------------------------------------------------------------------ #
print("Loading disease connection matrix...")
df = pd.read_csv(CONNECTION_MATRIX_PATH, index_col=0)

print("Loading codes mapping...")
codes_df = pd.read_csv(CODES_PATH, sep='\t')
code_to_name = dict(zip(codes_df['coding'], codes_df['meaning']))

diseases = df.index.tolist()

# Warn about missing mappings
missing_codes = [c for c in diseases if c not in code_to_name]
if missing_codes:
    print(f"Warning: {len(missing_codes)} codes not found in codes.tsv: {missing_codes[:10]}...")

# ------------------------------------------------------------------ #
# 2. Find top-10 co-occurrences (lower triangle)
# ------------------------------------------------------------------ #
print("Analyzing matrix for top 10 largest values...")
matrix = df.to_numpy(dtype=np.float64)

lower_vals = []
for i in range(len(diseases)):
    for j in range(i):
        val = matrix[i, j]
        if val > 0:
            lower_vals.append((val, diseases[i], diseases[j]))

lower_vals.sort(reverse=True)
top_10 = lower_vals[:10]

print("Top 10 largest values in the lower triangle:")
for val, d1, d2 in top_10:
    n1 = code_to_name.get(d1, d1)
    n2 = code_to_name.get(d2, d2)
    print(f"{d1} ({n1}) - {d2} ({n2}): {val}")

# ------------------------------------------------------------------ #
# 3. Build masked lower-triangle matrix + hover text
# ------------------------------------------------------------------ #
print("Processing lower triangle of the matrix...")
weight_threshold = 0
masked_matrix = np.zeros_like(matrix, dtype=np.float64)
hover_text = np.full_like(matrix, '', dtype=object)
non_zero = 0

for i in tqdm(range(len(diseases)), desc="Extracting lower triangle"):
    for j in range(i):
        val = matrix[i, j]
        if val > weight_threshold:
            masked_matrix[i, j] = val
            n1 = code_to_name.get(diseases[i], diseases[i])
            n2 = code_to_name.get(diseases[j], diseases[j])
            hover_text[i, j] = f"{diseases[j]} ({n2}) - {diseases[i]} ({n1}): {val}"
            non_zero += 1

print(f"Non-zero values in lower triangle (>{weight_threshold}): {non_zero}")

# ------------------------------------------------------------------ #
# 4. Create interactive heatmap
# ------------------------------------------------------------------ #
print("Creating interactive heatmap...")

# Calculate colorscale thresholds (convert numpy values to Python floats)
matrix_max = float(matrix.max())
threshold_value = 5000.0
threshold_normalized = float(threshold_value / matrix_max) if matrix_max > 0 else 1.0

# Ensure threshold is within valid range [0, 1]
threshold_normalized = min(max(threshold_normalized, 0.0), 1.0)

# Build colorscale with Python floats (not numpy types)
colorscale = [
    [0.0, 'black'],
    [1e-6, 'black'],
    [float(1e-6 + 1e-9), '#440154'],
    [threshold_normalized, '#FDE725'],
    [float(threshold_normalized + 1e-6) if threshold_normalized < 1.0 else 1.0, '#FF0000'],
    [1.0, '#FF0000']
]

fig = go.Figure(data=go.Heatmap(
    z=masked_matrix,
    x=diseases,
    y=diseases,
    text=hover_text,
    hoverinfo='text',
    colorscale=colorscale,
    zmin=0,
    zmax=matrix_max,
    colorbar=dict(title=dict(text='Co-occurrence Count', side='right'))
))

fig.update_layout(
    title="Interactive Disease Connection Heatmap (Lower Triangle, Zeros in Black, >5K in Red)",
    xaxis_title="Disease",
    yaxis_title="Disease",
    xaxis=dict(tickangle=45, tickfont=dict(size=8), side='top', automargin=True),
    yaxis=dict(tickfont=dict(size=8), autorange='reversed', automargin=True),
    width=1200,
    height=1200,
    margin=dict(l=150, r=150, t=200, b=150),
    hovermode='closest'
)

# ------------------------------------------------------------------ #
# 5. Save HTML
# ------------------------------------------------------------------ #
print("Saving interactive heatmap...")
fig.write_html(OUTPUT_HTML_PATH, auto_open=True)
print(f"Interactive heatmap saved to {OUTPUT_HTML_PATH}")