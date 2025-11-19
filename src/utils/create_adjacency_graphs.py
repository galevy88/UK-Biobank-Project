import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
import os

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# === CONFIGURATION ===
OLD_MATRIX_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/old_upper_ci_analysis_kobi_gal_session.csv'
YOUNG_MATRIX_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/young_upper_ci_analysis_kobi_gal_session.csv'
DISEASE_NAMES_PATH = 'data/disease_graph/input/disease_names_mapping_kobi_gal_session.yaml'
OUTPUT_DIR = 'data/disease_graph/output/kobi_gal_session/'

# ------------------------------------------------------------------ #
# Load disease names mapping
# ------------------------------------------------------------------ #
print("Loading disease names mapping...")
with open(DISEASE_NAMES_PATH, 'r') as file:
    code_to_name = yaml.safe_load(file)
if code_to_name is None:
    code_to_name = {}
print(f"  ✓ Loaded {len(code_to_name)} disease name mappings")

# ------------------------------------------------------------------ #
# Helper functions
# ------------------------------------------------------------------ #
def extract_edges_from_lower_triangle(df, threshold=0.0):
    """Extract edges from lower triangle of matrix."""
    matrix = df.values
    diseases = df.index.tolist()
    edges = []
    
    for i in range(len(diseases)):
        for j in range(i):  # Lower triangle: j < i
            val = matrix[i, j]
            if abs(val) > threshold:
                edges.append({
                    'source': diseases[j],
                    'target': diseases[i],
                    'weight': val
                })
    
    return edges, diseases


def wrap_text(text, max_chars=15):
    """Wrap text into multiple lines if too long."""
    words = text.split()
    lines = []
    current_line = []
    current_length = 0
    
    for word in words:
        word_length = len(word)
        # Check if adding this word would exceed max_chars
        if current_length + word_length + len(current_line) > max_chars and current_line:
            lines.append(' '.join(current_line))
            current_line = [word]
            current_length = word_length
        else:
            current_line.append(word)
            current_length += word_length
    
    if current_line:
        lines.append(' '.join(current_line))
    
    return '\n'.join(lines)


def create_circular_graph(df, title, code_to_name, global_min_weight, global_max_weight):
    """Create a circular network graph."""
    
    # Extract edges and nodes - only edges with values > 1
    edges, nodes = extract_edges_from_lower_triangle(df, threshold=1.0)
    
    print(f"\n  {title}:")
    print(f"    - Nodes: {len(nodes)}")
    print(f"    - Edges: {len(edges)}")
    
    # Create figure with larger size
    fig, ax = plt.subplots(figsize=(16, 16), facecolor='white')
    
    # Calculate circular positions
    n = len(nodes)
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False)
    radius = 2.5  # Increased from 1.0 to 1.5 - makes the circle bigger
    
    # Position each node
    positions = {}
    for i, node in enumerate(nodes):
        x = radius * np.cos(angles[i])
        y = radius * np.sin(angles[i])
        positions[node] = (x, y, angles[i])
    
    # Draw edges
    for edge in edges:
        source = edge['source']
        target = edge['target']
        weight = abs(edge['weight'])
        
        if source in positions and target in positions:
            x0, y0, _ = positions[source]
            x1, y1, _ = positions[target]
            
            # Scale line width based on weight
            if global_max_weight > global_min_weight:
                norm_weight = (weight - global_min_weight) / (global_max_weight - global_min_weight)
                line_width = 1.5 + 8.0 * norm_weight  # Range: 0.5 to 6.0
            else:
                line_width = 2.0
            
            # Draw edge in red
            ax.plot([x0, x1], [y0, y1], 'r-', linewidth=line_width, alpha=0.6, zorder=1)
    
    # Draw nodes
    for node in nodes:
        x, y, angle = positions[node]
        
        # Draw node circle
        circle = plt.Circle((x, y), 0.06, color='steelblue', ec='darkblue', 
                           linewidth=2, zorder=2)
        ax.add_patch(circle)
        
        # Calculate text position (outside the circle)
        # Push text further out by increasing the radius
        text_radius = radius + 0.8  # Text offset from circle (increased from 0.4 to 0.6)
        text_x = text_radius * np.cos(angle)
        text_y = text_radius * np.sin(angle)
        
        # Get disease name and wrap it
        disease_name = code_to_name.get(node, node)
        wrapped_name = wrap_text(disease_name, max_chars=15)
        
        # Determine text alignment based on angle
        # Convert angle to degrees for easier understanding
        angle_deg = np.degrees(angle) % 360
        
        # Horizontal alignment
        if angle_deg > 45 and angle_deg < 135:  # Bottom
            ha = 'center'
            va = 'top'
        elif angle_deg >= 135 and angle_deg < 225:  # Left
            ha = 'right'
            va = 'center'
        elif angle_deg >= 225 and angle_deg < 315:  # Top
            ha = 'center'
            va = 'bottom'
        else:  # Right (0-45 and 315-360)
            ha = 'left'
            va = 'center'
        
        # Draw text with wrapping
        ax.text(text_x, text_y, wrapped_name, fontsize=14, 
               ha=ha, va=va, fontweight='bold', zorder=3)
    
    # Add title with red wavy underline
    ax.text(0, 3.6, title, fontsize=22, fontweight='bold', 
           ha='center', va='bottom')
    
    
    # Set axis properties - expanded to accommodate larger circle and text
    ax.set_xlim(-4.0, 4.0)
    ax.set_ylim(-4.0, 4.0)
    ax.set_aspect('equal')
    ax.axis('off')
    
    return fig


# ------------------------------------------------------------------ #
# Main execution
# ------------------------------------------------------------------ #
print("\n" + "="*70)
print("Creating Adjacency Matrix Graphs")
print("="*70)

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load matrices
print("\nLoading matrices...")
old_df = pd.read_csv(OLD_MATRIX_PATH, index_col=0)
print(f"  ✓ Old matrix shape: {old_df.shape}")

# Try to load young matrix
if os.path.exists(YOUNG_MATRIX_PATH):
    young_df = pd.read_csv(YOUNG_MATRIX_PATH, index_col=0)
    print(f"  ✓ Young matrix shape: {young_df.shape}")
else:
    # Try upper as fallback
    fallback_path = YOUNG_MATRIX_PATH.replace('young_lower', 'young_upper')
    if os.path.exists(fallback_path):
        print(f"  ⚠ {YOUNG_MATRIX_PATH} not found, using: {fallback_path}")
        young_df = pd.read_csv(fallback_path, index_col=0)
        print(f"  ✓ Young matrix shape: {young_df.shape}")
    else:
        print(f"  ⚠ Error: Young matrix not found")
        young_df = None

# Calculate global min/max for consistent scaling (only edges > 1)
print("\nCalculating global weight range for edge width scaling...")
all_weights = []

if young_df is not None:
    young_edges, _ = extract_edges_from_lower_triangle(young_df, threshold=1.0)
    all_weights.extend([abs(e['weight']) for e in young_edges])

if old_df is not None:
    old_edges, _ = extract_edges_from_lower_triangle(old_df, threshold=1.0)
    all_weights.extend([abs(e['weight']) for e in old_edges])

if all_weights:
    global_min_weight = min(all_weights)
    global_max_weight = max(all_weights)
else:
    global_min_weight = 0.0
    global_max_weight = 1.0

print(f"  ✓ Global weight range: [{global_min_weight:.4f}, {global_max_weight:.4f}]")

# Create graphs
print("\nCreating graphs...")

if young_df is not None:
    print("\n  Creating Young graph...")
    fig_young = create_circular_graph(young_df, "Young", code_to_name, 
                                     global_min_weight, global_max_weight)
    young_output = os.path.join(OUTPUT_DIR, 'adjacency_graph_young.png')
    fig_young.savefig(young_output, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"  ✓ Saved: {young_output}")
    plt.close(fig_young)

if old_df is not None:
    print("\n  Creating Old graph...")
    fig_old = create_circular_graph(old_df, "Old", code_to_name,
                                   global_min_weight, global_max_weight)
    old_output = os.path.join(OUTPUT_DIR, 'adjacency_graph_old.png')
    fig_old.savefig(old_output, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"  ✓ Saved: {old_output}")
    plt.close(fig_old)

print("\n" + "="*70)
print("Done!")
print("="*70 + "\n")
