import re
import os
import yaml


def flatten_dict(input_dict: dict, prefix: str) -> dict:
    # Flattens one level of a dict, sets data types and adds a prefix to field names
    max_string_length = 4096
    flat_dict = {}
    for key, value in input_dict.items():
        if key.endswith('_id') or key == 'id': 
            flat_dict[prefix + key] = str(value)
        elif isinstance(value, int) or isinstance(value, float):
            flat_dict[prefix + key] = value 
        else:
            flat_dict[prefix + key] = str(value)[0:max_string_length]
    return flat_dict


def get_team_from_run(run: dict) -> str:
    # Generates an arbitrary run_team for filtering in NRQL queries. The commented
    # out code is what the New Relic enterprise data team uses. 
    team = 'Data Engineering' 
    # if run['project_id'] == '96622' and run['environment_id'] in ['86490', '86130']:
    #     team = 'Platform'
    # if re.match(r'Catch-all', run['job_name']):
    #     team = 'Project Catch All'
    return team


def extract_time_components(run: dict) -> int:
    # Run duration comes in a format of HH:MM:SS. We convert this to total seconds
    try:
        h, m, s = map(int, run['run_duration'].split(':'))
        total_seconds = h*3600+m*60+s
        return total_seconds
    except ValueError:
        return None


def read_config(file_name: str) -> dict:
        loc = os.path.dirname(__file__)
        conf_loc = os.path.join(loc, file_name)
        with open(conf_loc) as f_handle:
            try:
                config = yaml.safe_load(f_handle)
                return config
            except yaml.YAMLError as excp:
                raise excp