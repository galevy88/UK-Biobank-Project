import pandas as pd
import numpy as np
from itertools import combinations
from tqdm import tqdm
import os

# === CONFIGURATION ===
INPUT_DATA_PATH = 'data/data_hesin/data_hesin.csv'
OUTPUT_MATRIX_PATH = "data/disease_graph/intermediate_output/disease_connection_matrix.csv"
OUTPUT_COUNTS_PATH = "data/disease_graph/intermediate_output/disease_counts.csv"
EXCLUDED_DISEASES = {'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'}

# Create output directories if they don't exist
os.makedirs(os.path.dirname(OUTPUT_MATRIX_PATH), exist_ok=True)
print(f"Ensured output directory exists: {os.path.dirname(OUTPUT_MATRIX_PATH)}")

# ------------------------------------------------------------------ #
# 1. Load data
# ------------------------------------------------------------------ #
print(f"Loading data...")
df = pd.read_csv(INPUT_DATA_PATH)

# ------------------------------------------------------------------ #
# 2. Remove duplicate eid-diag_icd10 pairs
# ------------------------------------------------------------------ #
print(f"Checking for duplicates in data_hesin.csv...")
duplicate_count = df.duplicated(subset=['eid', 'diag_icd10']).sum()
print(f"Number of duplicate eid-diag_icd10 pairs: {duplicate_count}")
if duplicate_count > 0:
    print("Removing duplicates...")
    df = df.drop_duplicates(subset=['eid', 'diag_icd10'])

# ------------------------------------------------------------------ #
# 3. Basic stats
# ------------------------------------------------------------------ #
unique_eids = df['eid'].nunique()
print(f"Number of unique participants (eids): {unique_eids}")

# ------------------------------------------------------------------ #
# 4. Simplify ICD-10 codes (keep only the part before '.' or space)
# ------------------------------------------------------------------ #
print(f"Processing ICD10 codes...")
df['diag_icd10_simplified'] = df['diag_icd10'].str.split(r'[\.\s]').str[0]

# ------------------------------------------------------------------ #
# 5. Clean invalid / missing codes
# ------------------------------------------------------------------ #
df = df.dropna(subset=['diag_icd10_simplified'])
df['diag_icd10_simplified'] = df['diag_icd10_simplified'].astype(str)

# ------------------------------------------------------------------ #
# 6. Filter out unwanted prefixes
# ------------------------------------------------------------------ #
print(f"Filtering out ICD10 codes starting with {', '.join(sorted(EXCLUDED_DISEASES))}...")
df = df[~df['diag_icd10_simplified'].str.startswith(tuple(EXCLUDED_DISEASES))]
print(f"Remaining rows after filtering: {len(df)}")

# ------------------------------------------------------------------ #
# 7. Count individual disease occurrences (per participant)
# ------------------------------------------------------------------ #
print("Counting individual disease occurrences...")
disease_counts = (
    df.groupby('diag_icd10_simplified')['eid']
      .nunique()
      .rename_axis('disease')
      .reset_index(name='participant_count')
)
disease_counts = disease_counts.sort_values('participant_count', ascending=False)

print(f"Unique diseases after filtering: {len(disease_counts)}")
disease_counts.to_csv(OUTPUT_COUNTS_PATH, index=False)
print(f"Disease prevalence saved to {OUTPUT_COUNTS_PATH}")

# ------------------------------------------------------------------ #
# 8. Group diseases per patient (for co-occurrence matrix)
# ------------------------------------------------------------------ #
print(f"Grouping diseases by patient...")
patient_diseases = (
    df.groupby('eid')['diag_icd10_simplified']
      .apply(list)
      .reset_index()
)

# ------------------------------------------------------------------ #
# 9. Build co-occurrence matrix
# ------------------------------------------------------------------ #
unique_diseases = sorted(df['diag_icd10_simplified'].unique())
matrix_size = len(unique_diseases)
scoring_matrix = np.zeros((matrix_size, matrix_size), dtype=int)

disease_to_index = {disease: idx for idx, disease in enumerate(unique_diseases)}

print("Counting disease co-occurrences...")
for diseases in tqdm(patient_diseases['diag_icd10_simplified'],
                     desc="Processing co-occurrences"):
    for d1, d2 in combinations(set(diseases), 2):
        i1, i2 = disease_to_index[d1], disease_to_index[d2]
        scoring_matrix[i1, i2] += 1
        scoring_matrix[i2, i1] += 1

# ------------------------------------------------------------------ #
# 10. Save the symmetric co-occurrence matrix
# ------------------------------------------------------------------ #
scoring_df = pd.DataFrame(scoring_matrix,
                          index=unique_diseases,
                          columns=unique_diseases)
scoring_df.to_csv(OUTPUT_MATRIX_PATH)
print(f"Disease connection matrix saved to {OUTPUT_MATRIX_PATH}")