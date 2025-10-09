import dxpy
import yaml
import pandas as pd
import os


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def download_files_from_config(config: dict) -> str:
    directory = config["directory"]
    project_id = config['project']
    file_list = config['files']

    download_dir = os.path.join('../data', directory)
    os.makedirs(download_dir, exist_ok=True)

    for file_info in file_list:
        dxid = file_info['id']
        local_filename = file_info.get('local_name', f"{dxid}.csv")
        local_path = os.path.join(download_dir, local_filename)
        dxpy.download_dxfile(
            dxid=dxid,
            filename=local_path,
            project=project_id,
            show_progress=True
        )
    return os.path.abspath(download_dir)


def merge_csv_files(directory: str, merge_column: str = 'participant_id') -> pd.DataFrame:
    dfs = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            local_file = os.path.join(directory, filename)
            df = pd.read_csv(local_file)
            dfs.append(df)
    if not dfs:
        raise ValueError("No CSV files found in the directory")
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = pd.merge(combined_df, df, on=merge_column, how='outer')
    return combined_df


if __name__ == "__main__":
    config_path = '../configs/sanity_check.yaml'
    config = load_config(config_path)
    download_dir = download_files_from_config(config)
    combined_df = merge_csv_files(download_dir)
    print(combined_df.head())
