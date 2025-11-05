import pandas as pd
import numpy as np
from itertools import combinations
from tqdm import tqdm
import os


def process_disease_dataframe(df):
    """
    Process a DataFrame to create co-occurrence matrix.
    This function handles all the data processing logic.
    
    Args:
        df: DataFrame with columns 'eid' and 'diag_icd10'
    
    Returns:
        tuple: (scoring_df, unique_eids, unique_diseases)
            - scoring_df: DataFrame with co-occurrence matrix
            - unique_eids: Number of unique participants
            - unique_diseases: List of unique disease codes
    """
    # ------------------------------------------------------------------ #
    # 2. Remove duplicate eid-diag_icd10 pairs
    # ------------------------------------------------------------------ #
    print(f"\n[Step 2/8] Checking for duplicate eid-diag_icd10 pairs...")
    initial_rows = len(df)
    duplicate_count = df.duplicated(subset=['eid', 'diag_icd10']).sum()
    print(f"  Found {duplicate_count:,} duplicate pairs")
    if duplicate_count > 0:
        print("  Removing duplicates...")
        df = df.drop_duplicates(subset=['eid', 'diag_icd10'])
        removed = initial_rows - len(df)
        print(f"  ✓ Removed {removed:,} duplicate rows")
        print(f"  ✓ Remaining rows: {len(df):,}")
    else:
        print("  ✓ No duplicates found")
    
    # ------------------------------------------------------------------ #
    # 3. Basic stats
    # ------------------------------------------------------------------ #
    print(f"\n[Step 3/8] Calculating basic statistics...")
    unique_eids = df['eid'].nunique()
    unique_diseases_raw = df['diag_icd10'].nunique()
    print(f"  ✓ Unique participants (eids): {unique_eids:,}")
    print(f"  ✓ Unique disease codes (raw): {unique_diseases_raw:,}")
    print(f"  ✓ Total disease records: {len(df):,}")
    
    # ------------------------------------------------------------------ #
    # 4. Simplify ICD-10 codes (keep only the part before '.' or space)
    # ------------------------------------------------------------------ #
    print(f"\n[Step 4/8] Simplifying ICD-10 codes...")
    df['diag_icd10_simplified'] = df['diag_icd10'].str.split(r'[\.\s]').str[0]
    print(f"  ✓ Simplified ICD-10 codes (extracted prefix before '.' or space)")
    
    # ------------------------------------------------------------------ #
    # 5. Clean invalid / missing codes
    # ------------------------------------------------------------------ #
    print(f"\n[Step 5/8] Cleaning invalid/missing codes...")
    rows_before_clean = len(df)
    df = df.dropna(subset=['diag_icd10_simplified'])
    df['diag_icd10_simplified'] = df['diag_icd10_simplified'].astype(str)
    removed_invalid = rows_before_clean - len(df)
    if removed_invalid > 0:
        print(f"  ✓ Removed {removed_invalid:,} rows with invalid/missing codes")
    print(f"  ✓ Remaining rows after cleaning: {len(df):,}")
    
    # ------------------------------------------------------------------ #
    # 6. Group diseases per patient (for co-occurrence matrix)
    # ------------------------------------------------------------------ #
    print(f"\n[Step 6/8] Grouping diseases by patient...")
    patient_diseases = (
        df.groupby('eid')['diag_icd10_simplified']
          .apply(list)
          .reset_index()
    )
    print(f"  ✓ Grouped diseases for {len(patient_diseases):,} patients")
    # Calculate some stats
    disease_counts_per_patient = patient_diseases['diag_icd10_simplified'].apply(len)
    print(f"  ✓ Average diseases per patient: {disease_counts_per_patient.mean():.2f}")
    print(f"  ✓ Max diseases for a single patient: {disease_counts_per_patient.max()}")
    print(f"  ✓ Patients with multiple diseases: {(disease_counts_per_patient > 1).sum():,}")
    
    # ------------------------------------------------------------------ #
    # 7. Build co-occurrence matrix
    # ------------------------------------------------------------------ #
    print(f"\n[Step 7/8] Building co-occurrence matrix...")
    unique_diseases = sorted(df['diag_icd10_simplified'].unique())
    matrix_size = len(unique_diseases)
    scoring_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
    
    print(f"  Matrix size: {matrix_size:,} x {matrix_size:,} diseases")
    print(f"  Total possible pairs: {matrix_size * (matrix_size - 1) // 2:,}")
    
    disease_to_index = {disease: idx for idx, disease in enumerate(unique_diseases)}
    
    print("  Processing co-occurrences (this may take a while)...")
    total_co_occurrences = 0
    for diseases in tqdm(patient_diseases['diag_icd10_simplified'],
                         desc="  Processing co-occurrences"):
        for d1, d2 in combinations(set(diseases), 2):
            i1, i2 = disease_to_index[d1], disease_to_index[d2]
            scoring_matrix[i1, i2] += 1
            scoring_matrix[i2, i1] += 1
            total_co_occurrences += 1
    
    # Calculate matrix statistics
    non_zero_count = np.count_nonzero(scoring_matrix) // 2  # Divide by 2 because it's symmetric
    max_co_occurrence = np.max(scoring_matrix)
    print(f"\n  ✓ Matrix statistics:")
    print(f"    - Total co-occurrence pairs counted: {total_co_occurrences:,}")
    print(f"    - Unique disease pairs with co-occurrences: {non_zero_count:,}")
    print(f"    - Maximum co-occurrence count: {max_co_occurrence:,}")
    
    # Create DataFrame from scoring matrix
    scoring_df = pd.DataFrame(scoring_matrix,
                              index=unique_diseases,
                              columns=unique_diseases)
    
    return scoring_df, unique_eids, unique_diseases


def process_single_file(input_file_path, output_matrix_path):
    """
    Main function to process a single CSV file to create co-occurrence matrix.
    This function handles file I/O and delegates processing to process_disease_dataframe.
    
    Args:
        input_file_path: Path to a single input CSV file with disease data
        output_matrix_path: Path to save the disease connection matrix
    """
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(output_matrix_path), exist_ok=True)
    
    # ------------------------------------------------------------------ #
    # 1. Load data
    # ------------------------------------------------------------------ #
    print(f"\n[Step 1/8] Loading data from {input_file_path}...")
    df = pd.read_csv(input_file_path)
    print(f"  ✓ Loaded {len(df):,} rows from input file")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # ------------------------------------------------------------------ #
    # Process the DataFrame
    # ------------------------------------------------------------------ #
    scoring_df, unique_eids, unique_diseases = process_disease_dataframe(df)
    
    # ------------------------------------------------------------------ #
    # 8. Save the results
    # ------------------------------------------------------------------ #
    print(f"\n[Step 8/8] Saving co-occurrence matrix...")
    print(f"  Saving to {output_matrix_path}...")
    scoring_df.to_csv(output_matrix_path)
    file_size_mb = os.path.getsize(output_matrix_path) / (1024 * 1024)
    print(f"  ✓ Disease connection matrix saved successfully")
    print(f"  ✓ File size: {file_size_mb:.2f} MB")
    
    print("\n" + "="*70)
    print(f"Disease Score Processing Completed for {os.path.basename(input_file_path)}!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Input file: {input_file_path}")
    print(f"  - Unique diseases: {len(unique_diseases):,}")
    print(f"  - Unique participants: {unique_eids:,}")
    print(f"  - Co-occurrence matrix saved to: {output_matrix_path}")
    print("="*70 + "\n")


def disease_score_step(input_data_dir, output_base_dir, experiment_name):
    """
    Process all CSV files in a directory to create co-occurrence matrices.
    
    Args:
        input_data_dir: Path to directory containing CSV files to process
        output_base_dir: Base directory for saving outputs
        experiment_name: Experiment name to append to output directory
    """
    print("\n" + "="*70)
    print("Starting Disease Score Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Input directory: {input_data_dir}")
    print(f"Output base directory: {output_base_dir}")
    
    # Set working directory to src (parent of utils directory)
    SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(SRC_DIR)
    print(f"Working directory set to: {os.getcwd()}")
    
    # Create output directory with experiment name and connection_matrices subfolder
    output_dir = os.path.join(output_base_dir, experiment_name, "connection_matrices")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Find all CSV files in the input directory
    print(f"\n[Step 0] Scanning for CSV files in {input_data_dir}...")
    csv_files = [f for f in os.listdir(input_data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"  ⚠ Warning: No CSV files found in {input_data_dir}")
        return
    
    print(f"  ✓ Found {len(csv_files):,} CSV file(s): {', '.join(csv_files)}")
    
    # Process each CSV file
    for csv_file in csv_files:
        input_file_path = os.path.join(input_data_dir, csv_file)
        
        # Generate output filename based on input filename
        # e.g., "old_filtered.csv" -> "old_disease_connection_matrix.csv"
        base_name = os.path.splitext(csv_file)[0]  # Remove .csv extension
        
        # Remove "_filtered" suffix if present for cleaner names
        if base_name.endswith('_filtered'):
            base_name = base_name[:-9]  # Remove "_filtered"
        
        output_matrix_file = f"{base_name}_disease_connection_matrix.csv"
        output_matrix_path = os.path.join(output_dir, output_matrix_file)
        
        print(f"\n{'='*70}")
        print(f"Processing: {csv_file}")
        print(f"{'='*70}")
        
        # Process the file
        process_single_file(input_file_path, output_matrix_path)
    
    print("\n" + "="*70)
    print("Disease Score Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Processed {len(csv_files):,} file(s)")
    print(f"  - Input directory: {input_data_dir}")
    print(f"  - Output directory: {output_dir}")
    print("="*70 + "\n")


if __name__ == '__main__':
    # Default configuration for direct execution
    INPUT_DATA_DIR = 'data/pipelines/z_score_pipeline/exp1/'
    OUTPUT_BASE_DIR = "data/pipelines/z_score_pipeline/"
    EXPERIMENT_NAME = "exp1"
    
    disease_score_step(INPUT_DATA_DIR, OUTPUT_BASE_DIR, EXPERIMENT_NAME)