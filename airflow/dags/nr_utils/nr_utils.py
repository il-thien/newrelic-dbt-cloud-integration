import re


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
    '''
    Allows us to provide our own logic to set run_team in dbt_job_run custom event. 
    This function has access to all attributes returned in the dbt job run.
      - projec_name
      - environment_name
      - All fields listed in the (dbt Cloud v2 API for runs)[https://docs.getdbt.com/dbt-cloud/api-v2#/operations/Retrieve%20Run]. 
        All attributes are prepended with "run_"
    '''
    team = 'Data Engineering'
    # team = 'Data Engineering' 
    # if run['project_id'] == '96622' and run['environment_id'] in ['86490', '86130']:
    #     team = 'Platform'
    # if re.match(r'Catch-all', run['job_name']):
    #     team = 'Project Catch All'
    return team


