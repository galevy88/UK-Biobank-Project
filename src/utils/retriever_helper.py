import os
import yaml
import pandas as pd

def load_csv(file_path, required_columns=None):
    """Load a CSV file and return a DataFrame, handling file not found errors."""
    try:
        data = pd.read_csv(file_path)
        if required_columns:
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                raise ValueError(f"Missing columns in {file_path}: {missing_cols}")
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: {file_path} not found. Please check the file path.")


def get_disease_eids_by_type(hesin_data, disease_name):
    """Return set of participant IDs with specified disease based on ICD10 codes from YAML file."""
    # Load ICD10 codes from YAML file
    with open('configs/diseases_mapping.yaml', 'r') as file:
        disease_codes = yaml.safe_load(file)

    # Get the list of ICD10 codes for the specified disease
    icd10_codes = disease_codes.get(disease_name, [])

    # Return set of EIDs matching the ICD10 codes
    return set(hesin_data[
                   hesin_data['diag_icd10'].notna() &
                   hesin_data['diag_icd10'].str.startswith(tuple(icd10_codes))
                   ]['eid'].unique())

def get_disease_eids(hesin_data):
    """Return set of participant IDs with any disease diagnosis (ICD10 A00-Q99)."""
    return set(hesin_data[
        hesin_data['diag_icd10'].notna() &
        hesin_data['diag_icd10'].str.match('^[A-Q][0-9][0-9]')
    ]['eid'].unique())

def get_healthy_eids(all_eids, disease_eids):
    """Return set of healthy participant IDs (no A00-Q99 diagnoses)."""
    return all_eids - disease_eids


def save_output(final_df, config, output_dir, base_name):
    """
    Save the final DataFrame and config YAML to the specified directory.

    Args:
        final_df (pd.DataFrame): The combined participant data.
        config (dict): The loaded configuration dictionary.
        output_dir (str): Directory to save files (will be created if not exists).
        base_name (str): Base name for output files (without extension).
    """
    os.makedirs(output_dir, exist_ok=True)

    csv_output = os.path.join(output_dir, f"{base_name}.csv")
    yaml_output = os.path.join(output_dir, f"{base_name}.yaml")

    # Save CSV
    final_df.to_csv(csv_output, index=False)
    print(f"\nData saved to: {csv_output}")

    # Save config copy
    with open(yaml_output, 'w') as f:
        yaml.safe_dump(config, f, sort_keys=False)
    print(f"Config saved to: {yaml_output}")

    print(f"Total participants in combined file: {len(final_df)}")
    print(f"Unique labels: {final_df['label'].unique()}")
    print(f"Label distribution:\n{final_df['label'].value_counts()}")