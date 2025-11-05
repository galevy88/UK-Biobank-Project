import pandas as pd
import numpy as np
import os
import glob


def bootstrap_step(experiment_name, input_data_dir, output_base_dir, fields_to_keep, shuffle_iterations, save_bootstrap_data=True):
    """
    Bootstrap step that processes filtered data by keeping only specified fields,
    creating multiple shuffled versions (one per shuffle) and saving each as a separate file.
    
    Args:
        experiment_name: Name of the experiment
        input_data_dir: Directory containing filtered CSV files to process
        output_base_dir: Base directory for saving outputs
        fields_to_keep: List of field names to keep in the DataFrame
        shuffle_iterations: Number of shuffled files to create (each with one shuffle)
        save_bootstrap_data: Boolean flag to save bootstrap data to disk (default: True)
    
    Returns:
        list: List of all shuffled DataFrames created during the bootstrap process
    """
    print("\n" + "="*70)
    print("Starting Bootstrap Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Input directory: {input_data_dir}")
    print(f"Output base directory: {output_base_dir}")
    print(f"Fields to keep: {fields_to_keep}")
    print(f"Number of bootstrap files to create: {shuffle_iterations}")
    print(f"Save bootstrap data: {save_bootstrap_data}")
    
    # Create base output directory
    base_output_dir = os.path.join(output_base_dir, experiment_name, "bootstraped_hesin_data")
    if save_bootstrap_data:
        os.makedirs(base_output_dir, exist_ok=True)
        print(f"Base output directory: {base_output_dir}")
    
    # Find all CSV files in the input directory
    print(f"\n[Step 1/3] Scanning for CSV files in {input_data_dir}...")
    csv_files = glob.glob(os.path.join(input_data_dir, "*.csv"))
    
    if not csv_files:
        print(f"  ⚠ Warning: No CSV files found in {input_data_dir}")
        return []
    
    print(f"  ✓ Found {len(csv_files):,} CSV file(s)")
    for csv_file in csv_files:
        print(f"    - {os.path.basename(csv_file)}")
    
    total_files_created = 0
    all_shuffled_dfs = []  # List to store all shuffled DataFrames
    
    # Process each CSV file
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        base_name = os.path.splitext(file_name)[0]  # Remove .csv extension
        
        print(f"\n{'='*70}")
        print(f"Processing: {file_name}")
        print(f"{'='*70}")
        
        # Create subfolder for this bootstrap data type
        type_output_dir = os.path.join(base_output_dir, base_name)
        if save_bootstrap_data:
            os.makedirs(type_output_dir, exist_ok=True)
            print(f"  Type output directory: {type_output_dir}")
        
        # Load the CSV file
        print(f"\n[Step 2/3] Loading and processing {file_name}...")
        df = pd.read_csv(csv_file)
        print(f"  ✓ Loaded {len(df):,} rows")
        print(f"  ✓ Original columns: {list(df.columns)}")
        print(f"  ✓ Original columns count: {len(df.columns)}")
        
        # Check if all required fields exist
        available_fields = [field for field in fields_to_keep if field in df.columns]
        missing_fields = [field for field in fields_to_keep if field not in df.columns]
        if missing_fields:
            print(f"  ⚠ Warning: Missing fields in DataFrame: {missing_fields}")
            print(f"  ⚠ Using only available fields...")
        
        if not available_fields:
            print(f"  ⚠ Error: None of the required fields are available! Skipping this file.")
            continue
        
        # Keep only specified fields
        df_base = df[available_fields].copy()
        print(f"  ✓ Kept {len(available_fields)} fields: {available_fields}")
        print(f"  ✓ Remaining rows: {len(df_base):,}")
        
        # Check if diag_icd10 column exists
        if 'diag_icd10' not in df_base.columns:
            print(f"  ⚠ Warning: 'diag_icd10' column not found! Cannot shuffle.")
            print(f"  ⚠ Creating files without shuffling...")
            diag_icd10_available = False
        else:
            diag_icd10_available = True
        
        # Create multiple shuffled versions
        print(f"\n[Step 3/3] Creating {shuffle_iterations} bootstrap files...")
        original_diag_icd10 = df_base['diag_icd10'].copy() if diag_icd10_available else None
        
        for i in range(1, shuffle_iterations + 1):
            # Create a copy of the base DataFrame
            df_shuffled = df_base.copy()
            
            # Shuffle diag_icd10 column once
            if diag_icd10_available:
                df_shuffled['diag_icd10'] = np.random.permutation(original_diag_icd10.values)
            
            # Add the shuffled DataFrame to the list
            all_shuffled_dfs.append(df_shuffled.copy())
            
            # Save the bootstrapped data if flag is True
            if save_bootstrap_data:
                # Create output filename with index
                output_file = os.path.join(type_output_dir, f"{base_name}_{i}.csv")
                
                # Save the bootstrapped data
                df_shuffled.to_csv(output_file, index=False)
                total_files_created += 1
                
                # Print progress every 10 files or for first/last file
                if i == 1 or i == shuffle_iterations or i % 10 == 0:
                    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
                    print(f"  ✓ Created file {i}/{shuffle_iterations}: {os.path.basename(output_file)} ({file_size_mb:.2f} MB)")
            else:
                # Just count without saving
                total_files_created += 1
                if i == 1 or i == shuffle_iterations or i % 10 == 0:
                    print(f"  ✓ Generated bootstrap {i}/{shuffle_iterations} (not saved)")
        
        if save_bootstrap_data:
            print(f"  ✓ Completed creating {shuffle_iterations} bootstrap files for {file_name}")
        else:
            print(f"  ✓ Completed generating {shuffle_iterations} bootstrap versions for {file_name} (not saved)")
    
    print("\n" + "="*70)
    print("Bootstrap Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Processed {len(csv_files):,} input file(s)")
    if save_bootstrap_data:
        print(f"  - Created {total_files_created:,} bootstrap file(s)")
    else:
        print(f"  - Generated {total_files_created:,} bootstrap version(s) (not saved)")
    print(f"  - Total shuffled DataFrames in memory: {len(all_shuffled_dfs):,}")
    print(f"  - Input directory: {input_data_dir}")
    if save_bootstrap_data:
        print(f"  - Output directory: {base_output_dir}")
    print(f"  - Fields kept: {fields_to_keep}")
    print(f"  - Bootstrap files per input: {shuffle_iterations}")
    print(f"  - Save bootstrap data: {save_bootstrap_data}")
    print("="*70 + "\n")
    
    return all_shuffled_dfs

