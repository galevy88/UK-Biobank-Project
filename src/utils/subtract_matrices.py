import pandas as pd
import numpy as np
import os
from pathlib import Path

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# === CONFIGURATION ===
MATRIX_1_PATH = 'data/pipelines/z_score_pipeline/kobi_track/ci_analysis/old_upper_ci_analysis.csv'
MATRIX_2_PATH = 'data/pipelines/z_score_pipeline/kobi_track/ci_analysis/young_upper_ci_analysis.csv'

# Generate dynamic output filename based on input filenames
def generate_output_path(matrix1_path, matrix2_path):
    """Generate output path with dynamic filename based on input matrix names."""
    # Extract base names without extension
    name1 = Path(matrix1_path).stem
    name2 = Path(matrix2_path).stem
    
    # Output directory: same as matrix1's directory (ci_analysis)
    base_dir = os.path.dirname(matrix1_path)
    
    # Generate filename: name1_minus_name2.csv
    output_filename = f"{name1}_minus_{name2}.csv"
    output_path = os.path.join(base_dir, output_filename)
    
    return output_path

OUTPUT_PATH = generate_output_path(MATRIX_1_PATH, MATRIX_2_PATH)

# ------------------------------------------------------------------ #
# 1. Load matrices
# ------------------------------------------------------------------ #
print(f"Loading matrix 1 from: {MATRIX_1_PATH}")
df1 = pd.read_csv(MATRIX_1_PATH, index_col=0)

print(f"Loading matrix 2 from: {MATRIX_2_PATH}")
df2 = pd.read_csv(MATRIX_2_PATH, index_col=0)

# ------------------------------------------------------------------ #
# 2. Analyze and identify diseases to drop
# ------------------------------------------------------------------ #
print(f"\nMatrix 1 shape: {df1.shape}")
print(f"Matrix 2 shape: {df2.shape}")

# Identify diseases that are in one matrix but not the other
indices1 = set(df1.index)
indices2 = set(df2.index)
cols1 = set(df1.columns)
cols2 = set(df2.columns)

# Diseases only in matrix 1 (rows or columns)
only_in_1_rows = indices1 - indices2
only_in_1_cols = cols1 - cols2

# Diseases only in matrix 2 (rows or columns)
only_in_2_rows = indices2 - indices1
only_in_2_cols = cols2 - cols1

# Combine all diseases that need to be dropped
# Drop diseases that are in one matrix but not the other
diseases_to_drop = (only_in_1_rows | only_in_1_cols | only_in_2_rows | only_in_2_cols)

if diseases_to_drop:
    print("\n" + "="*60)
    print("IDENTIFYING DISEASES TO DROP:")
    print("="*60)
    
    print(f"\nTotal diseases in matrix 1 (rows): {len(indices1)}")
    print(f"Total diseases in matrix 2 (rows): {len(indices2)}")
    print(f"Total diseases in matrix 1 (columns): {len(cols1)}")
    print(f"Total diseases in matrix 2 (columns): {len(cols2)}")
    
    if only_in_1_rows or only_in_1_cols:
        print(f"\nDiseases ONLY in matrix 1 (will be dropped from matrix 1):")
        diseases_in_1_only = sorted(only_in_1_rows | only_in_1_cols)
        for disease in diseases_in_1_only:
            locations = []
            if disease in only_in_1_rows:
                locations.append("rows")
            if disease in only_in_1_cols:
                locations.append("columns")
            print(f"  - {disease} (in {', '.join(locations)})")
    
    if only_in_2_rows or only_in_2_cols:
        print(f"\nDiseases ONLY in matrix 2 (will be dropped from matrix 2):")
        diseases_in_2_only = sorted(only_in_2_rows | only_in_2_cols)
        for disease in diseases_in_2_only:
            locations = []
            if disease in only_in_2_rows:
                locations.append("rows")
            if disease in only_in_2_cols:
                locations.append("columns")
            print(f"  - {disease} (in {', '.join(locations)})")
    
    # Drop diseases from both matrices
    print(f"\nDropping {len(diseases_to_drop)} disease(s) from both matrices...")
    
    # Drop from matrix 1 (both rows and columns)
    diseases_to_drop_from_1 = diseases_to_drop & (indices1 | cols1)
    if diseases_to_drop_from_1:
        # Drop rows
        rows_to_drop = [d for d in diseases_to_drop_from_1 if d in df1.index]
        if rows_to_drop:
            print(f"  Dropping {len(rows_to_drop)} row(s) from matrix 1: {sorted(rows_to_drop)}")
            df1 = df1.drop(index=rows_to_drop)
        
        # Drop columns
        cols_to_drop = [d for d in diseases_to_drop_from_1 if d in df1.columns]
        if cols_to_drop:
            print(f"  Dropping {len(cols_to_drop)} column(s) from matrix 1: {sorted(cols_to_drop)}")
            df1 = df1.drop(columns=cols_to_drop)
    
    # Drop from matrix 2 (both rows and columns)
    diseases_to_drop_from_2 = diseases_to_drop & (indices2 | cols2)
    if diseases_to_drop_from_2:
        # Drop rows
        rows_to_drop = [d for d in diseases_to_drop_from_2 if d in df2.index]
        if rows_to_drop:
            print(f"  Dropping {len(rows_to_drop)} row(s) from matrix 2: {sorted(rows_to_drop)}")
            df2 = df2.drop(index=rows_to_drop)
        
        # Drop columns
        cols_to_drop = [d for d in diseases_to_drop_from_2 if d in df2.columns]
        if cols_to_drop:
            print(f"  Dropping {len(cols_to_drop)} column(s) from matrix 2: {sorted(cols_to_drop)}")
            df2 = df2.drop(columns=cols_to_drop)
    
    # Ensure both matrices have the same diseases in the same order
    common_diseases = sorted(set(df1.index) & set(df2.index))
    df1 = df1.loc[common_diseases, common_diseases]
    df2 = df2.loc[common_diseases, common_diseases]
    
    print(f"\n✓ Both matrices now have {len(common_diseases)} diseases in the same order")
else:
    print("\n✓ Both matrices have identical diseases - no dropping needed")
    
    # Ensure same order
    common_diseases = sorted(set(df1.index) & set(df2.index))
    df1 = df1.loc[common_diseases, common_diseases]
    df2 = df2.loc[common_diseases, common_diseases]

print("\n" + "="*60)
print(f"FINAL ALIGNED MATRIX DIMENSIONS: {df1.shape}")
print(f"Number of rows (diseases): {len(df1.index)}")
print(f"Number of columns (diseases): {len(df1.columns)}")
print("="*60)

# ------------------------------------------------------------------ #
# 3. Perform subtraction: matrix1 - matrix2
# ------------------------------------------------------------------ #
print("Performing subtraction: matrix1 - matrix2...")
result = df1 - df2

# ------------------------------------------------------------------ #
# 4. Display statistics
# ------------------------------------------------------------------ #
print("\nResult statistics:")
print(f"  Min value: {result.min().min():.6f}")
print(f"  Max value: {result.max().max():.6f}")
print(f"  Mean value: {result.mean().mean():.6f}")
print(f"  Number of positive values: {(result > 0).sum().sum()}")
print(f"  Number of negative values: {(result < 0).sum().sum()}")
print(f"  Number of zero values: {(result == 0).sum().sum()}")

# ------------------------------------------------------------------ #
# 5. Ensure output directory exists and save
# ------------------------------------------------------------------ #
output_dir = os.path.dirname(OUTPUT_PATH)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists: {output_dir}")

print(f"Saving result to: {OUTPUT_PATH}")
result.to_csv(OUTPUT_PATH)
print(f"Successfully saved result matrix to {OUTPUT_PATH}")

