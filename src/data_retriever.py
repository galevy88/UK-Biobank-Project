import pandas as pd
import yaml
import os
from utils.retriever_helper import (
    load_csv, get_disease_eids_by_type, get_disease_eids, get_healthy_eids, save_output
)

script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "configs/selections/heart_diseases_selection.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

save_data = config.get("save_data", False)
disease_selection = config.get("disease_selection", {})
HESIN_DATA_PATH = os.path.join(script_dir, config.get("HESIN_DATA_PATH", ""))
PARTICIPANT_DATA_PATH = os.path.join(script_dir, config.get("PARTICIPANT_DATA_PATH", ""))
OUTPUT_PATH = os.path.join(script_dir, config.get("OUTPUT_PATH", "").strip())
DISEASES_MAPPING_PATH = os.path.join(script_dir, config.get("DISEASES_MAPPING_PATH", ""))
remove_prefix = config.get("remove_prefix")
participant_id_column = config.get("participant_id_column", "eid")

try:
    hesin_data = load_csv(HESIN_DATA_PATH, required_columns=["eid", "diag_icd10"])
    hesin_data["diag_icd10"] = (hesin_data["diag_icd10"].astype(str).replace("nan", pd.NA))

    all_participants = load_csv(PARTICIPANT_DATA_PATH)

    if remove_prefix and isinstance(remove_prefix, str):
        all_participants.columns = [col.replace(remove_prefix, "") for col in all_participants.columns]

    all_eids = set(all_participants[participant_id_column].unique())

    disease_eids_dict = {}
    disease_counts = {}

    for disease, active in disease_selection.items():
        if active and disease != "healthy":
            disease_eids = get_disease_eids_by_type(hesin_data, disease, DISEASES_MAPPING_PATH)
            disease_eids_dict[disease] = disease_eids
            disease_counts[disease] = len(disease_eids)

    if disease_selection.get("healthy", False):
        all_disease_eids = get_disease_eids(hesin_data)
        healthy_eids = get_healthy_eids(all_eids, all_disease_eids)
        disease_eids_dict["healthy"] = healthy_eids
        disease_counts["healthy"] = len(healthy_eids)

    combined_data = []
    for disease, eids in disease_eids_dict.items():
        disease_df = all_participants[all_participants[participant_id_column].isin(eids)].copy()
        disease_df["label"] = disease.replace("_", " ").title()
        combined_data.append(disease_df)

    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)

        if save_data and OUTPUT_PATH:
            save_output(final_df, config, OUTPUT_PATH)
    else:
        final_df = pd.DataFrame()
        print("No participant data to save.")

except (FileNotFoundError, ValueError) as e:
    print(f"Error: {str(e)}")