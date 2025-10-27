import pandas as pd
import numpy as np
from itertools import combinations
from tqdm import tqdm

# Read the CSV file
print(f"Loading data...")
df = pd.read_csv('data/disease_graph/data_hesin.csv')

# Check for duplicates
print(f"Checking for duplicates in data_hesin.csv...")
duplicate_count = df.duplicated(subset=['eid', 'diag_icd10']).sum()
print(f"Number of duplicate eid-diag_icd10 pairs: {duplicate_count}")
if duplicate_count > 0:
    print("Removing duplicates...")
    df = df.drop_duplicates(subset=['eid', 'diag_icd10'])

# Print number of unique participants
unique_eids = df['eid'].nunique()
print(f"Number of unique participants (eids): {unique_eids}")

# Simplify ICD10 codes by taking the first part before any dot or space
print(f"Processing ICD10 codes...")
df['diag_icd10_simplified'] = df['diag_icd10'].str.split(r'[\.\s]').str[0]

# Handle missing or invalid ICD10 codes
df = df.dropna(subset=['diag_icd10_simplified'])  # Remove rows with NaN
df['diag_icd10_simplified'] = df['diag_icd10_simplified'].astype(str)  # Ensure all values are strings

# Filter out codes starting with R, S, T, U, V, W, X, Y, or Z
print(f"Filtering out ICD10 codes starting with R through Z...")
df = df[~df['diag_icd10_simplified'].str.startswith(('R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'))]
print(f"Remaining rows after filtering: {len(df)}")

# Group by eid to get all diseases for each patient
print(f"Grouping diseases by patient...")
patient_diseases = df.groupby('eid')['diag_icd10_simplified'].apply(list).reset_index()

# Get unique diseases
print(f"Identifying unique diseases...")
unique_diseases = sorted(df['diag_icd10_simplified'].unique())

# Initialize scoring matrix
matrix_size = len(unique_diseases)
scoring_matrix = np.zeros((matrix_size, matrix_size), dtype=int)

# Create disease index mapping
disease_to_index = {disease: idx for idx, disease in enumerate(unique_diseases)}

# Count disease co-occurrences with progress bar
for diseases in tqdm(patient_diseases['diag_icd10_simplified'], desc="Processing disease co-occurrences"):
    # Get all possible pairs of diseases for this patient
    for disease1, disease2 in combinations(set(diseases), 2):  # Use set to avoid double-counting
        idx1 = disease_to_index[disease1]
        idx2 = disease_to_index[disease2]
        # Increment both directions in the matrix (symmetric)
        scoring_matrix[idx1][idx2] += 1
        scoring_matrix[idx2][idx1] += 1


# Create a DataFrame for the scoring matrix
scoring_df = pd.DataFrame(scoring_matrix, index=unique_diseases, columns=unique_diseases)

# Save the scoring matrix to a CSV file
scoring_df.to_csv('data/disease_graph/disease_connection_matrix.csv')
print("Disease connection matrix saved to 'data/disease_graph/disease_connection_matrix.csv'")