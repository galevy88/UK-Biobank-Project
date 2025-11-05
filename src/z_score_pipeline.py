import yaml
import os
from steps.z_score_pipeline.filter_step import filter_step
from utils.disease_score import disease_score_step

def read_config():
    # Set working directory to src (script directory)
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(SRC_DIR)
    with open('configs/z_score_pipeline.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config


if __name__ == '__main__':
    config = read_config()

    # Filter step configuration
    experiment_name = config['filter_step']['experiment_name']
    hesin_data_path = config['filter_step']['HESIN_DATA_PATH']
    codes_path = config['filter_step']['CODES_PATH']
    method = config['filter_step']['method']
    filter_path = config['filter_step']['FILTER_PATH']
    output_path = config['filter_step']['OUTPUT_PATH']
    filteration = config['filter_step']['filteration']

    # Run filter step
    filter_step(experiment_name, hesin_data_path, codes_path, method, filter_path, output_path, filteration)

    # Disease score step configuration
    if 'disease_score_step' in config:
        input_data_dir = config['disease_score_step']['INPUT_DATA_DIR']
        output_base_dir = config['disease_score_step']['OUTPUT_BASE_DIR']
        
        # Run disease score step (using experiment_name from filter_step)
        disease_score_step(input_data_dir, output_base_dir, experiment_name)

