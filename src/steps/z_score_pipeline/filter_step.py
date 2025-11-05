import pandas as pd
import yaml
import os
from itertools import product



def filter_step(experiment_name, hesin_data_path, codes_path, method, filter_path, output_path, filteration):
    print("\n" + "="*70)
    print("Starting Filter Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Filter method: {method}")
    
    # Load the wanted disease codes from YAML
    print(f"\n[Step 1/5] Loading disease codes from {codes_path}...")
    with open(codes_path, 'r') as file:
        disease_codes = yaml.safe_load(file)['codes']
    print(f"  ✓ Loaded {len(disease_codes):,} disease codes")
    if len(disease_codes) <= 10:
        print(f"  ✓ Codes: {', '.join(disease_codes)}")
    else:
        print(f"  ✓ First 10 codes: {', '.join(list(disease_codes)[:10])}...")

    # Load the datasets
    print(f"\n[Step 2/5] Loading datasets...")
    print(f"  Loading HESIN data from {hesin_data_path}...")
    hesin_df = pd.read_csv(hesin_data_path)
    original_hesin_rows = len(hesin_df)
    print(f"  ✓ Loaded {original_hesin_rows:,} rows from HESIN data")
    print(f"  ✓ HESIN columns: {list(hesin_df.columns)}")
    
    print(f"  Loading filter data from {filter_path}...")
    eid_age_sex_df = pd.read_csv(filter_path)
    print(f"  ✓ Loaded {len(eid_age_sex_df):,} rows from filter data")
    print(f"  ✓ Filter columns: {list(eid_age_sex_df.columns)}")
    
    output_path = f"{output_path}/{experiment_name}/filtered_data"
    print(f"\n  Output directory: {output_path}")

    # Filter hesin_df based on disease codes
    print(f"\n[Step 3/5] Filtering HESIN data by disease codes...")
    rows_before_filter = len(hesin_df)
    if method == 'keep':
        hesin_df = hesin_df[hesin_df['diag_icd10'].str.split().str[0].isin(disease_codes)]
        print(f"  ✓ Filtering method: KEEP (only codes in the list)")
    elif method == 'drop':
        hesin_df = hesin_df[~hesin_df['diag_icd10'].str.split().str[0].isin(disease_codes)]
        print(f"  ✓ Filtering method: DROP (exclude codes in the list)")
    else:
        print(f"  ⚠ Warning: Unknown method '{method}', skipping disease code filtering")
    
    rows_after_filter = len(hesin_df)
    removed = rows_before_filter - rows_after_filter
    print(f"  ✓ Rows before filtering: {rows_before_filter:,}")
    print(f"  ✓ Rows after filtering: {rows_after_filter:,}")
    print(f"  ✓ Rows removed: {removed:,} ({removed/rows_before_filter*100:.1f}%)")

    # Merge with eid_age_sex_df to get additional fields
    print(f"\n[Step 4/5] Merging HESIN data with filter data...")
    merged_df = hesin_df.merge(eid_age_sex_df, on='eid', how='inner')
    print(f"  ✓ Merged dataset: {len(merged_df):,} rows")
    print(f"  ✓ Merged columns: {list(merged_df.columns)}")
    print(f"  ✓ Unique participants (eids): {merged_df['eid'].nunique():,}")

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    print(f"  ✓ Output directory created/verified: {output_path}")

    # Check if filteration is None or the string "None"
    print(f"\n[Step 5/5] Applying filters and saving results...")
    if filteration is None or filteration == "None":
        # No filtering, save the merged DataFrame
        print(f"  No additional filtering specified, saving merged data...")
        output_file = os.path.join(output_path, 'filtered_data.csv')
        merged_df.to_csv(output_file, index=False)
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"  ✓ Saved unfiltered data to {output_file}")
        print(f"  ✓ Records: {len(merged_df):,}")
        print(f"  ✓ File size: {file_size_mb:.2f} MB")
    else:
        # Function to generate filter combinations
        def generate_filter_combinations(filteration):
            filter_groups = []
            for field, groups in filteration.items():
                group_conditions = []
                for group_name, condition in groups.items():
                    if isinstance(condition, dict) and 'min' in condition and 'max' in condition:
                        # Range-based condition (e.g., for age)
                        group_conditions.append((field, group_name, {'min': condition['min'], 'max': condition['max']}))
                    else:
                        # Categorical condition (e.g., for sex)
                        group_conditions.append((field, group_name, condition))
                filter_groups.append(group_conditions)
            # Generate all possible combinations of filter groups
            return list(product(*filter_groups))

        # Generate all filter combinations
        print(f"  Generating filter combinations...")
        filter_combinations = generate_filter_combinations(filteration)
        print(f"  ✓ Generated {len(filter_combinations):,} filter combination(s)")
        
        # Print filter details
        print(f"  Filter configuration:")
        for field, groups in filteration.items():
            print(f"    - {field}:")
            for group_name, condition in groups.items():
                if isinstance(condition, dict) and 'min' in condition and 'max' in condition:
                    print(f"      * {group_name}: {condition['min']} <= value <= {condition['max']}")
                else:
                    print(f"      * {group_name}: {condition}")

        # Apply filters and save each group
        print(f"\n  Applying filters and saving results...")
        total_records_saved = 0
        for idx, combination in enumerate(filter_combinations, 1):
            # Create a copy of the DataFrame to apply filters
            group_df = merged_df.copy()
            group_name_parts = []
            filter_details = []

            # Apply each filter in the combination
            for field, group_name, condition in combination:
                group_name_parts.append(group_name)
                if isinstance(condition, dict) and 'min' in condition and 'max' in condition:
                    # Apply range filter
                    rows_before = len(group_df)
                    group_df = group_df[
                        (group_df[field] >= condition['min']) &
                        (group_df[field] <= condition['max'])
                        ]
                    rows_after = len(group_df)
                    filter_details.append(f"{field} [{condition['min']}-{condition['max']}]: {rows_before:,} -> {rows_after:,} rows")
                else:
                    # Apply categorical filter
                    rows_before = len(group_df)
                    group_df = group_df[group_df[field] == condition]
                    rows_after = len(group_df)
                    filter_details.append(f"{field} == {condition}: {rows_before:,} -> {rows_after:,} rows")

            # Create group name by joining parts
            group_name = '_'.join(group_name_parts)

            # Save to CSV
            output_file = os.path.join(output_path, f'{group_name}_filtered.csv')
            group_df.to_csv(output_file, index=False)
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            total_records_saved += len(group_df)
            
            print(f"\n  [{idx}/{len(filter_combinations)}] {group_name}:")
            for detail in filter_details:
                print(f"    {detail}")
            print(f"    ✓ Saved to {output_file}")
            print(f"    ✓ Records: {len(group_df):,}")
            print(f"    ✓ Unique participants: {group_df['eid'].nunique():,}")
            print(f"    ✓ File size: {file_size_mb:.2f} MB")
        
        print(f"\n  Summary:")
        print(f"    ✓ Total filter combinations processed: {len(filter_combinations):,}")
        print(f"    ✓ Total records saved across all files: {total_records_saved:,}")

    print("\n" + "="*70)
    print("Filter Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Experiment: {experiment_name}")
    print(f"  - Output directory: {output_path}")
    print(f"  - Input HESIN records: {original_hesin_rows:,}")
    print(f"  - Final merged records: {len(merged_df):,}")
    print(f"  - Unique participants: {merged_df['eid'].nunique():,}")
    print("="*70 + "\n")