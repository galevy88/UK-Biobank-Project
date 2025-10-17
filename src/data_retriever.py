import pandas as pd
from utils.loader import load_csv, get_disease_eids_by_type, get_disease_eids, get_healthy_eids, print_results
import os

save_data = True

# Dictionary to specify which diseases and healthy population to analyze
disease_selection = {
    'heart_disease': 'Yes',
    'lung_disease': 'No',
    'diabetes': 'No',
    'cancer': 'No',
    'hypertension': 'No',
    'stroke': 'No',
    'kidney_disease': 'No',
    'liver_disease': 'No',
    'mental_disorders': 'No',
    'cognitive_disorders': 'No',
    'inflammatory_diseases': 'No',
    'digestive_system_diseases': 'No',
    'healthy': 'Yes'
}

try:
    # Load data
    hesin_data = load_csv("data/sanity_check/data_hesin.csv", required_columns=['eid', 'diag_icd10'])
    hesin_data['diag_icd10'] = hesin_data['diag_icd10'].astype(str).replace('nan', pd.NA)

    all_participants = load_csv("data/sanity_check/ukb_participant.csv", required_columns=['eid'])
    all_eids = set(all_participants['eid'].unique())

    # Get EIDs for selected diseases and store in dictionary
    disease_eids_dict = {}
    disease_counts = {}
    for disease, active in disease_selection.items():
        if active.lower() == 'yes' and disease != 'healthy':
            disease_eids = get_disease_eids_by_type(hesin_data, disease)
            disease_eids_dict[disease] = disease_eids
            disease_counts[disease] = len(disease_eids)

    # Get all disease EIDs (A00-Q99) and healthy EIDs if healthy is selected
    if disease_selection.get('healthy', 'No').lower() == 'yes':
        all_disease_eids = get_disease_eids(hesin_data)
        healthy_eids = get_healthy_eids(all_eids, all_disease_eids)
        disease_eids_dict['healthy'] = healthy_eids
        disease_counts['healthy'] = len(healthy_eids)

    # Retrieve participant data based on EIDs and add label column
    combined_data = []
    for disease, eids in disease_eids_dict.items():
        disease_df = all_participants[all_participants['eid'].isin(eids)].copy()
        disease_df['label'] = disease.replace('_', ' ').title()  # Convert to title case for readability
        combined_data.append(disease_df)

    # Print results
    print_results(disease_counts)

    # Combine all dataframes
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)

        # Save to CSV if save_data is True
        if save_data:
            output_dir = "data/sanity_check/output"
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, "participant_data_by_disease.csv")
            final_df.to_csv(output_file, index=False)
            print(f"\nData saved to: {output_file}")
            print(f"Total participants in combined file: {len(final_df)}")
            print(f"Unique labels: {final_df['label'].unique()}")
            print(f"Label distribution:\n{final_df['label'].value_counts()}")
    else:
        final_df = pd.DataFrame()
        print("No participant data to save.")



except (FileNotFoundError, ValueError) as e:
    print(f"Error: {str(e)}")