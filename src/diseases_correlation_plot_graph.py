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
CONNECTION_MATRIX_PATH = 'data/pipelines/z_score_pipeline/kobi_track/ci_analysis/old_upper_ci_analysis_minus_young_upper_ci_analysis.csv'
CODES_PATH = 'data/disease_graph/input/codes.tsv'
OUTPUT_HTML_PATH = "data/disease_graph/output/disease_connection_heatmap_kobi_track_old_minus_young.html"

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
print("Analyzing matrix for top 10 largest absolute values...")
matrix = df.to_numpy(dtype=np.float64)

lower_vals = []
for i in range(len(diseases)):
    for j in range(i):
        val = matrix[i, j]
        # Include all values (positive and negative)
        lower_vals.append((abs(val), val, diseases[i], diseases[j]))

lower_vals.sort(reverse=True)
top_10 = lower_vals[:10]

print("Top 10 largest absolute values in the lower triangle:")
for abs_val, val, d1, d2 in top_10:
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
        n1 = code_to_name.get(diseases[i], diseases[i])
        n2 = code_to_name.get(diseases[j], diseases[j])
        # Include all values (positive, negative, and zero)
        masked_matrix[i, j] = val
        hover_text[i, j] = f"{diseases[j]} ({n2}) - {diseases[i]} ({n1}): {val:.6f}"
        if val != 0:
            non_zero += 1

print(f"Non-zero values in lower triangle (>{weight_threshold}): {non_zero}")

# ------------------------------------------------------------------ #
# 4. Create interactive heatmap
# ------------------------------------------------------------------ #
print("Creating interactive heatmap...")

# Calculate matrix min and max (convert numpy values to Python floats)
matrix_min = float(matrix.min())
matrix_max = float(matrix.max())
matrix_range = matrix_max - matrix_min

print(f"Matrix value range: [{matrix_min:.6f}, {matrix_max:.6f}]")

# Normalize thresholds -1 and 1 to [0, 1] range for colorscale
# Formula: normalized_value = (value - matrix_min) / (matrix_max - matrix_min)
def normalize_value(val):
    if matrix_range == 0:
        return 0.5
    return (val - matrix_min) / matrix_range

normalized_minus_1 = normalize_value(-1.0) if matrix_min < -1.0 else None
normalized_1 = normalize_value(1.0) if matrix_max > 1.0 else None
normalized_0 = normalize_value(0.0)

epsilon = 1e-6
dark_gray = '#1a1a1a'  # Very dark gray, much closer to black

# Build colorscale with three regions:
# - Values < -1: blue scale (tendency to appear more in young)
# - Values between -1 to 1: black-gray scale
# - Values > 1: red scale (tendency to appear more in old)
colorscale = []

# Determine the actual range we need to cover
actual_start = 0.0
actual_end = 1.0

# Handle values below -1 (blue scale)
if normalized_minus_1 is not None and normalized_minus_1 > 0:
    # Blue gradient from dark blue to lighter blue
    colorscale.append([0.0, '#000080'])  # Dark blue for minimum value
    if normalized_minus_1 > epsilon * 2:
        colorscale.append([max(epsilon, normalized_minus_1 * 0.5), '#4169E1'])  # Royal blue
    colorscale.append([normalized_minus_1, '#87CEEB'])  # Sky blue at -1
    actual_start = 0.0
else:
    # If no values below -1, start from the actual minimum
    actual_start = 0.0
    colorscale.append([0.0, dark_gray])  # Start with dark gray

# Handle values between -1 and 1 (black-gray scale)
start_gray = normalized_minus_1 if normalized_minus_1 is not None else 0.0
end_gray = normalized_1 if normalized_1 is not None else 1.0

# Black-gray gradient for -1 to 1 range
if start_gray < end_gray:
    if start_gray > actual_start:
        colorscale.append([start_gray, dark_gray])  # Dark gray at -1
    # Add black at 0 if 0 is within the range
    if normalized_0 is not None and max(start_gray, actual_start) < normalized_0 < end_gray:
        colorscale.append([normalized_0, 'black'])  # Black at 0
    colorscale.append([end_gray, dark_gray])  # Dark gray at 1

# Handle values above 1 (red scale)
if normalized_1 is not None and normalized_1 < 1.0:
    # Red gradient from dark red to bright red
    colorscale.append([min(normalized_1 + epsilon, 1.0), '#8B0000'])  # Dark red just above 1
    mid_red = normalized_1 + (1.0 - normalized_1) * 0.5
    if mid_red < 1.0 - epsilon:
        colorscale.append([mid_red, '#DC143C'])  # Crimson
    colorscale.append([1.0, '#FF0000'])  # Bright red for maximum value
    actual_end = 1.0
else:
    # If no values above 1, end with dark gray
    if end_gray < 1.0:
        colorscale.append([1.0, dark_gray])

# Ensure colorscale is sorted by first element and remove duplicates
colorscale.sort(key=lambda x: x[0])
# Remove duplicate positions (keep the last one for each position)
unique_colorscale = []
seen_positions = set()
for pos, color in reversed(colorscale):
    if pos not in seen_positions:
        unique_colorscale.append([pos, color])
        seen_positions.add(pos)
colorscale = sorted(unique_colorscale, key=lambda x: x[0])

# Ensure we have entries at 0.0 and 1.0
if colorscale[0][0] > 0.0:
    # Find the color at the start
    colorscale.insert(0, [0.0, colorscale[0][1]])
if colorscale[-1][0] < 1.0:
    # Find the color at the end
    colorscale.append([1.0, colorscale[-1][1]])

print(f"Colorscale thresholds: -1 normalized to {normalized_minus_1}, 1 normalized to {normalized_1}")

fig = go.Figure(data=go.Heatmap(
    z=masked_matrix,
    x=diseases,
    y=diseases,
    text=hover_text,
    hoverinfo='text',
    colorscale=colorscale,
    zmin=matrix_min,
    zmax=matrix_max,
    colorbar=dict(
        title=dict(
            text='Difference (Old - Young)<br><span style="font-size:10px;">Blue (< -1): More in Young | Gray (-1 to 1): Similar | Red (> 1): More in Old</span>',
            side='right'
        )
    )
))

fig.update_layout(
    title="Interactive Disease Connection Heatmap (Lower Triangle: Blue=Young, Gray=Similar, Red=Old)",
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