import pandas as pd
import yaml
import os

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# === CONFIGURATION ===
SELECTED_DISEASES_PATH = 'data/pipelines/z_score_pipeline/codes_files/kobi_gal_session.yaml'
MATRIX_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/old_upper_ci_analysis.csv'
OUTPUT_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/old_upper_ci_analysis_kobi_gal_session.csv'

# ------------------------------------------------------------------ #
# 1. Load selected diseases
# ------------------------------------------------------------------ #
print(f"Loading selected diseases from: {SELECTED_DISEASES_PATH}")
with open(SELECTED_DISEASES_PATH, 'r') as file:
    selected_diseases_data = yaml.safe_load(file)
selected_codes = selected_diseases_data.get('codes', [])
print(f"  ✓ Loaded {len(selected_codes)} selected codes")
print(f"  ✓ Selected codes: {', '.join(selected_codes)}")

# ------------------------------------------------------------------ #
# 2. Load matrix
# ------------------------------------------------------------------ #
print(f"\nLoading matrix from: {MATRIX_PATH}")
df = pd.read_csv(MATRIX_PATH, index_col=0)
print(f"  ✓ Loaded matrix with shape: {df.shape}")
print(f"  ✓ Matrix has {len(df.index)} rows and {len(df.columns)} columns")

# Get all codes in the matrix (both rows and columns should be the same)
matrix_codes = set(df.index) | set(df.columns)
print(f"  ✓ Unique codes in matrix: {len(matrix_codes)}")

# ------------------------------------------------------------------ #
# 3. Find matching codes (including subcodes)
# ------------------------------------------------------------------ #
print(f"\nFinding matching codes (including subcodes)...")
codes_to_keep = set()

for selected_code in selected_codes:
    matching_codes = []
    
    # Check for exact match
    if selected_code in matrix_codes:
        matching_codes.append(selected_code)
        codes_to_keep.add(selected_code)
    
    # Find all subcodes (codes that start with selected_code followed by a dot)
    prefix = selected_code + '.'
    for code in matrix_codes:
        if code.startswith(prefix):
            matching_codes.append(code)
            codes_to_keep.add(code)
    
    if matching_codes:
        print(f"  ✓ {selected_code}: Found {len(matching_codes)} code(s) - {', '.join(sorted(matching_codes)[:10])}{'...' if len(matching_codes) > 10 else ''}")
    else:
        print(f"  ⚠ {selected_code}: No matching codes found in matrix")

codes_to_keep = sorted(list(codes_to_keep))
print(f"\n  ✓ Total codes to keep: {len(codes_to_keep)}")

# ------------------------------------------------------------------ #
# 4. Filter matrix to keep only selected codes
# ------------------------------------------------------------------ #
print(f"\nFiltering matrix...")
print(f"  Original matrix shape: {df.shape}")

# Filter rows and columns to keep only codes_to_keep
# Keep only codes that exist in both index and columns
available_codes = sorted(list(set(codes_to_keep) & set(df.index) & set(df.columns)))
print(f"  Codes available in both rows and columns: {len(available_codes)}")

if len(available_codes) == 0:
    raise ValueError("No matching codes found in the matrix! Please check the selected codes and matrix.")

filtered_df = df.loc[available_codes, available_codes]
print(f"  Filtered matrix shape: {filtered_df.shape}")

# ------------------------------------------------------------------ #
# 5. Save filtered matrix
# ------------------------------------------------------------------ #
print(f"\nSaving filtered matrix to: {OUTPUT_PATH}")
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
filtered_df.to_csv(OUTPUT_PATH)
print(f"  ✓ Successfully saved filtered matrix")

# ------------------------------------------------------------------ #
# 6. Summary
# ------------------------------------------------------------------ #
print("\n" + "="*70)
print("Summary:")
print("="*70)
print(f"  - Selected codes: {len(selected_codes)}")
print(f"  - Total codes in original matrix: {len(matrix_codes)}")
print(f"  - Codes to keep (including subcodes): {len(codes_to_keep)}")
print(f"  - Codes available in matrix: {len(available_codes)}")
print(f"  - Original matrix shape: {df.shape}")
print(f"  - Filtered matrix shape: {filtered_df.shape}")
print(f"  - Output file: {OUTPUT_PATH}")
print("="*70 + "\n")

