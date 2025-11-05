import yaml
from steps.z_score_pipeline.filter_step import filter_step

def read_config():
    with open('configs/z_score_pipeline.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config


if __name__ == '__main__':
    config = read_config()

    experiment_name = config['filter_step']['experiment_name']
    hesin_data_path = config['filter_step']['HESIN_DATA_PATH']
    codes_path = config['filter_step']['CODES_PATH']
    method = config['filter_step']['method']
    filter_path = config['filter_step']['FILTER_PATH']
    output_path = config['filter_step']['OUTPUT_PATH']
    filteration = config['filter_step']['filteration']

    filter_step(experiment_name, hesin_data_path, codes_path, method, filter_path, output_path, filteration)


