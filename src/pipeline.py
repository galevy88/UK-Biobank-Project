import pandas as pd
import yaml
import os
from itertools import product

# Load the YAML configuration file
with open('configs/disease_graph_pipeline.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Extract configuration parameters
hesin_data_path = config['filter_step']['HESIN_DATA_PATH']
codes_path = config['filter_step']['CODES_PATH']
method = config['filter_step']['method']
filter_path = config['filter_step']['FILTER_PATH']
output_path = config['filter_step']['OUTPUT_PATH']
filteration = config['filter_step']['filteration']

# Load the wanted disease codes from YAML
with open(codes_path, 'r') as file:
    disease_codes = yaml.safe_load(file)['codes']

# Load the datasets
hesin_df = pd.read_csv(hesin_data_path)
eid_age_sex_df = pd.read_csv(filter_path)

# Filter hesin_df based on disease codes
if method == 'keep':
    hesin_df = hesin_df[hesin_df['diag_icd10'].str.split().str[0].isin(disease_codes)]
elif method == 'drop':
    hesin_df = hesin_df[~hesin_df['diag_icd10'].str.split().str[0].isin(disease_codes)]

# Merge with eid_age_sex_df to get additional fields
merged_df = hesin_df.merge(eid_age_sex_df, on='eid', how='inner')

# Ensure output directory exists
os.makedirs(output_path, exist_ok=True)

# Check if filteration is None or the string "None"
if filteration is None or filteration == "None":
    # No filtering, save the merged DataFrame
    output_file = os.path.join(output_path, 'filtered_data.csv')
    merged_df.to_csv(output_file, index=False)
    print(f'Saved unfiltered data to {output_file} with {len(merged_df)} records')
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
    filter_combinations = generate_filter_combinations(filteration)

    # Apply filters and save each group
    for combination in filter_combinations:
        # Create a copy of the DataFrame to apply filters
        group_df = merged_df.copy()
        group_name_parts = []

        # Apply each filter in the combination
        for field, group_name, condition in combination:
            group_name_parts.append(group_name)
            if isinstance(condition, dict) and 'min' in condition and 'max' in condition:
                # Apply range filter
                group_df = group_df[
                    (group_df[field] >= condition['min']) &
                    (group_df[field] <= condition['max'])
                ]
            else:
                # Apply categorical filter
                group_df = group_df[group_df[field] == condition]

        # Create group name by joining parts
        group_name = '_'.join(group_name_parts)

        # Save to CSV
        output_file = os.path.join(output_path, f'{group_name}_filtered.csv')
        group_df.to_csv(output_file, index=False)
        print(f'Saved {group_name} to {output_file} with {len(group_df)} records')

print("Processing complete.")