import pandas as pd
import numpy as np
from itertools import combinations
from tqdm import tqdm
import os


def process_disease_dataframe(df, verbose=False):
    """
    Process a DataFrame to create co-occurrence matrix.
    This function handles all the data processing logic.
    
    Args:
        df: DataFrame with columns 'eid' and 'diag_icd10'
        verbose: If True, print detailed progress (default: False)
    
    Returns:
        tuple: (scoring_df, unique_eids, unique_diseases)
            - scoring_df: DataFrame with co-occurrence matrix
            - unique_eids: Number of unique participants
            - unique_diseases: List of unique disease codes
    """
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # ------------------------------------------------------------------ #
    # 2. Remove duplicate eid-diag_icd10 pairs
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 2/8] Checking for duplicate eid-diag_icd10 pairs...")
    initial_rows = len(df)
    duplicate_count = df.duplicated(subset=['eid', 'diag_icd10']).sum()
    if verbose:
        print(f"  Found {duplicate_count:,} duplicate pairs")
    if duplicate_count > 0:
        if verbose:
            print("  Removing duplicates...")
        df = df.drop_duplicates(subset=['eid', 'diag_icd10']).copy()
        removed = initial_rows - len(df)
        if verbose:
            print(f"  ✓ Removed {removed:,} duplicate rows")
            print(f"  ✓ Remaining rows: {len(df):,}")
    elif verbose:
        print("  ✓ No duplicates found")
    
    # ------------------------------------------------------------------ #
    # 3. Basic stats
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 3/8] Calculating basic statistics...")
    unique_eids = df['eid'].nunique()
    unique_diseases_raw = df['diag_icd10'].nunique()
    if verbose:
        print(f"  ✓ Unique participants (eids): {unique_eids:,}")
        print(f"  ✓ Unique disease codes (raw): {unique_diseases_raw:,}")
        print(f"  ✓ Total disease records: {len(df):,}")
    
    # ------------------------------------------------------------------ #
    # 4. Simplify ICD-10 codes (keep only the part before '.' or space)
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 4/8] Simplifying ICD-10 codes...")
    df['diag_icd10_simplified'] = df['diag_icd10'].str.split(r'[\.\s]').str[0]
    if verbose:
        print(f"  ✓ Simplified ICD-10 codes (extracted prefix before '.' or space)")
    
    # ------------------------------------------------------------------ #
    # 5. Clean invalid / missing codes
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 5/8] Cleaning invalid/missing codes...")
    rows_before_clean = len(df)
    df = df.dropna(subset=['diag_icd10_simplified']).copy()
    df['diag_icd10_simplified'] = df['diag_icd10_simplified'].astype(str)
    removed_invalid = rows_before_clean - len(df)
    if verbose and removed_invalid > 0:
        print(f"  ✓ Removed {removed_invalid:,} rows with invalid/missing codes")
    if verbose:
        print(f"  ✓ Remaining rows after cleaning: {len(df):,}")
    
    # ------------------------------------------------------------------ #
    # 6. Group diseases per patient (for co-occurrence matrix)
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 6/8] Grouping diseases by patient...")
    patient_diseases = (
        df.groupby('eid')['diag_icd10_simplified']
          .apply(list)
          .reset_index()
    )
    if verbose:
        print(f"  ✓ Grouped diseases for {len(patient_diseases):,} patients")
        # Calculate some stats
        disease_counts_per_patient = patient_diseases['diag_icd10_simplified'].apply(len)
        print(f"  ✓ Average diseases per patient: {disease_counts_per_patient.mean():.2f}")
        print(f"  ✓ Max diseases for a single patient: {disease_counts_per_patient.max()}")
        print(f"  ✓ Patients with multiple diseases: {(disease_counts_per_patient > 1).sum():,}")
    
    # ------------------------------------------------------------------ #
    # 7. Build co-occurrence matrix
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 7/8] Building co-occurrence matrix...")
    unique_diseases = sorted(df['diag_icd10_simplified'].unique())
    matrix_size = len(unique_diseases)
    scoring_matrix = np.zeros((matrix_size, matrix_size), dtype=int)
    
    if verbose:
        print(f"  Matrix size: {matrix_size:,} x {matrix_size:,} diseases")
        print(f"  Total possible pairs: {matrix_size * (matrix_size - 1) // 2:,}")
    
    disease_to_index = {disease: idx for idx, disease in enumerate(unique_diseases)}
    
    total_co_occurrences = 0
    # Always show progress bar for co-occurrence processing (it's the slowest step)
    for diseases in tqdm(patient_diseases['diag_icd10_simplified'],
                         desc="  Co-occurrences",
                         leave=False):
        for d1, d2 in combinations(set(diseases), 2):
            i1, i2 = disease_to_index[d1], disease_to_index[d2]
            scoring_matrix[i1, i2] += 1
            scoring_matrix[i2, i1] += 1
            total_co_occurrences += 1
    
    if verbose:
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


def process_single_dataframe(df, output_matrix_path, dataset_name="dataset", verbose=False):
    """
    Process a DataFrame to create co-occurrence matrix.
    This function handles DataFrame processing and saving.
    
    Args:
        df: DataFrame with disease data
        output_matrix_path: Path to save the disease connection matrix
        dataset_name: Name identifier for the dataset (for logging)
        verbose: If True, print detailed progress (default: False)
    """
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(output_matrix_path), exist_ok=True)
    
    # ------------------------------------------------------------------ #
    # Process the DataFrame
    # ------------------------------------------------------------------ #
    scoring_df, unique_eids, unique_diseases = process_disease_dataframe(df, verbose=verbose)
    
    # ------------------------------------------------------------------ #
    # Save the results
    # ------------------------------------------------------------------ #
    if verbose:
        print(f"\n[Step 8/8] Saving co-occurrence matrix...")
        print(f"  Saving to {output_matrix_path}...")
    scoring_df.to_csv(output_matrix_path)
    if verbose:
        file_size_mb = os.path.getsize(output_matrix_path) / (1024 * 1024)
        print(f"  ✓ Disease connection matrix saved successfully")
        print(f"  ✓ File size: {file_size_mb:.2f} MB")
        print("\n" + "="*70)
        print(f"Disease Score Processing Completed for {dataset_name}!")
        print("="*70)
        print(f"Summary:")
        print(f"  - Dataset: {dataset_name}")
        print(f"  - Unique diseases: {len(unique_diseases):,}")
        print(f"  - Unique participants: {unique_eids:,}")
        print(f"  - Co-occurrence matrix saved to: {output_matrix_path}")
        print("="*70 + "\n")


def process_single_file(input_file_path, output_matrix_path):
    """
    Main function to process a single CSV file to create co-occurrence matrix.
    This function handles file I/O and delegates processing.
    
    Args:
        input_file_path: Path to a single input CSV file with disease data
        output_matrix_path: Path to save the disease connection matrix
    """
    df = pd.read_csv(input_file_path)
    dataset_name = os.path.basename(input_file_path)
    process_single_dataframe(df, output_matrix_path, dataset_name)


def connection_matrices_step(original_data_dir, shuffled_dfs, output_base_dir, experiment_name, shuffle_iterations):
    """
    Process original and shuffled DataFrames to create co-occurrence matrices.
    
    Args:
        original_data_dir: Path to directory containing original filtered CSV files
        shuffled_dfs: List of shuffled DataFrames from bootstrap step
        output_base_dir: Base directory for saving outputs
        experiment_name: Experiment name to append to output directory
        shuffle_iterations: Number of bootstrap iterations (for organizing output)
    """
    print("\n" + "="*70)
    print("Starting Connection Matrices Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Original data directory: {original_data_dir}")
    print(f"Output base directory: {output_base_dir}")
    print(f"Number of shuffled DataFrames: {len(shuffled_dfs)}")
    
    # Set working directory to src (parent of steps directory)
    # File is at: src/steps/z_score_pipeline/connection_matrices_step.py
    # Need to go up 3 levels: z_score_pipeline -> steps -> src
    SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.chdir(SRC_DIR)
    print(f"Working directory set to: {os.getcwd()}")
    
    # Resolve original_data_dir to absolute path if it's relative
    if not os.path.isabs(original_data_dir):
        original_data_dir = os.path.abspath(original_data_dir)
    print(f"Resolved original data directory: {original_data_dir}")
    
    # Create output directories
    base_output_dir = os.path.join(output_base_dir, experiment_name, "connection_matrices")
    original_output_dir = os.path.join(base_output_dir, "original")
    bootstrap_output_dir = os.path.join(base_output_dir, "bootstrap")
    
    os.makedirs(original_output_dir, exist_ok=True)
    os.makedirs(bootstrap_output_dir, exist_ok=True)
    
    print(f"Base output directory: {base_output_dir}")
    print(f"Original matrices directory: {original_output_dir}")
    print(f"Bootstrap matrices directory: {bootstrap_output_dir}")
    
    total_processed = 0
    
    # ------------------------------------------------------------------ #
    # Process original DataFrames
    # ------------------------------------------------------------------ #
    print(f"\n[Step 1/2] Processing original DataFrames...")
    import glob
    original_csv_files = glob.glob(os.path.join(original_data_dir, "*.csv"))
    
    if not original_csv_files:
        print(f"  ⚠ Warning: No CSV files found in {original_data_dir}")
    else:
        print(f"  ✓ Found {len(original_csv_files):,} original file(s)")
        
        # Store original DataFrames and their names for later reference
        original_dataframes = {}
        
        # Process original files with progress bar
        for csv_file in tqdm(original_csv_files, desc="Processing original matrices"):
            file_name = os.path.basename(csv_file)
            base_name = os.path.splitext(file_name)[0]  # Remove .csv extension
            
            # Remove "_filtered" suffix if present for cleaner names
            if base_name.endswith('_filtered'):
                base_name = base_name[:-9]  # Remove "_filtered"
            
            df = pd.read_csv(csv_file)
            original_dataframes[base_name] = df
            
            # Create output path
            output_matrix_file = f"{base_name}_disease_connection_matrix.csv"
            output_matrix_path = os.path.join(original_output_dir, output_matrix_file)
            
            # Process and save (quiet mode)
            process_single_dataframe(df, output_matrix_path, f"original_{base_name}", verbose=False)
            total_processed += 1
    
    # ------------------------------------------------------------------ #
    # Process shuffled DataFrames
    # ------------------------------------------------------------------ #
    print(f"\n[Step 2/2] Processing shuffled DataFrames...")
    
    if not shuffled_dfs:
        print(f"  ⚠ Warning: No shuffled DataFrames provided")
    else:
        print(f"  ✓ Processing {len(shuffled_dfs):,} shuffled DataFrame(s)")
        
        # Group shuffled DataFrames by their source type
        # We need to infer the type from the order: first N are from first file, next N from second file, etc.
        if original_csv_files:
            num_original_files = len(original_csv_files)
            dfs_per_file = len(shuffled_dfs) // num_original_files
            
            for file_idx, csv_file in enumerate(original_csv_files):
                file_name = os.path.basename(csv_file)
                base_name = os.path.splitext(file_name)[0]
                
                # Remove "_filtered" suffix if present
                if base_name.endswith('_filtered'):
                    base_name = base_name[:-9]
                
                # Create subdirectory for this bootstrap type
                type_bootstrap_dir = os.path.join(bootstrap_output_dir, base_name)
                os.makedirs(type_bootstrap_dir, exist_ok=True)
                
                # Get the shuffled DataFrames for this file
                start_idx = file_idx * dfs_per_file
                end_idx = start_idx + dfs_per_file
                file_shuffled_dfs = shuffled_dfs[start_idx:end_idx]
                
                print(f"\n  Processing bootstrap matrices for: {base_name} ({len(file_shuffled_dfs):,} versions)")
                
                # Process each shuffled DataFrame with progress bar
                for bootstrap_idx, df_shuffled in enumerate(tqdm(file_shuffled_dfs, 
                                                                  desc=f"  {base_name} bootstrap",
                                                                  leave=False), 1):
                    output_matrix_file = f"{base_name}_bootstrap_{bootstrap_idx}_disease_connection_matrix.csv"
                    output_matrix_path = os.path.join(type_bootstrap_dir, output_matrix_file)
                    
                    # Process and save (quiet mode)
                    process_single_dataframe(
                        df_shuffled, 
                        output_matrix_path, 
                        f"bootstrap_{base_name}_{bootstrap_idx}",
                        verbose=False
                    )
                    total_processed += 1
    
    print("\n" + "="*70)
    print("Connection Matrices Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Processed {total_processed:,} dataset(s) total")
    print(f"  - Original datasets: {len(original_csv_files) if original_csv_files else 0}")
    print(f"  - Bootstrap datasets: {len(shuffled_dfs)}")
    print(f"  - Output directory: {base_output_dir}")
    print("="*70 + "\n")


if __name__ == '__main__':
    # Default configuration for direct execution
    INPUT_DATA_DIR = 'data/pipelines/z_score_pipeline/exp1/'
    OUTPUT_BASE_DIR = "data/pipelines/z_score_pipeline/"
    EXPERIMENT_NAME = "exp1"
    
    connection_matrices_step(INPUT_DATA_DIR, OUTPUT_BASE_DIR, EXPERIMENT_NAME)

