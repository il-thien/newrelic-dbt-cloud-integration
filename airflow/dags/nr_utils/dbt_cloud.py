import re
from airflow.providers.http.hooks.http import HttpHook


def dbt_cloud_validation(responses):
    # Validates the standard response from dbt Cloud API
    for response in responses:
        if 'data' not in response.json() and isinstance(response.json()['data'], list):
            print(f'Response from dbt cloud could not be parsed')
            return False 
    return True


def dbt_cloud_response_filter(responses):
    # Concatenate data from the list of paginated responses 
    data = []
    for response in responses:
        data += response.json()['data']
    return data


def dbt_cloud_secure_response_filter(responses):
    # Project data can contain sensative data. We only get the name
    data = {} 
    for response in responses:
        for item in response.json()['data']:
            data[item['id']] = { 'id': item['id'], 'name': item['name'] }
    return data


def paginate_dbt_cloud_api_response(response):
        payload = response.json()
        extras = payload['extra']
        total_count = extras['pagination']['total_count']
        count = extras['pagination']['count']
        offset = extras['filters']['offset']
        if count + offset < total_count:
            return dict(data={'offset': offset + count }) 


def get_dbt_cloud_manifest_filtered(manifest: dict) -> dict:
    fields = []
    for node_id, node_data in manifest.get('nodes', {}).items():

        test_model = node_data.get('test_metadata', {}).get('kwargs', {}).get('model') 
        match = re.search(r"\'(\w+)\'", test_model) if test_model else None
        model_name = match.group(1) if match else node_data.get('name') 
        failed_test_rows_limit = node_data.get('config', {}).get('meta',{}).get('nr_config',{}).get('failed_test_row_limit',100)
        
        fields.append({
            'resource_type': node_data.get('resource_type'),
            'unique_id': node_data.get('unique_id'),
            'database_name': node_data.get('database'),
            'schema_name': node_data.get('schema'),
            'test_column_name': node_data.get('test_metadata', {}).get('kwargs', {}).get('column_name'),
            'test_model_name': model_name,
            'test_namespace': node_data.get('test_metadata', {}).get('namespace'),
            'test_parameters': node_data.get('test_metadata', {}).get('kwargs'),
            'test_short_name': node_data.get('test_metadata', {}).get('name'),
            'alias': node_data.get('alias'),
            'severity': node_data.get('config', {}).get('severity'),
            'warn_if': node_data.get('config', {}).get('warn_if'),
            'error_if': node_data.get('config', {}).get('error_id'),
            'tags': node_data.get('config', {}).get('tags'),
            'path': node_data.get('path'),
            'original_file_path': node_data.get('original_file_path'),
            'meta': node_data.get('meta'),
            'meta_config': node_data.get('config', {}).get('meta'),
            'team': node_data.get('config', {}).get('meta',{}).get('nr_config',{}).get('team','Data Engineering'),
            'alert_failed_test_rows':node_data.get('config', {}).get('meta',{}).get('nr_config',{}).get('alert_failed_test_rows',False),
            'failed_test_row_limit': failed_test_rows_limit if failed_test_rows_limit<=100 else 100,
            'slack_mentions': node_data.get('config', {}).get('meta',{}).get('nr_config',{}).get('slack_mentions'),
            'message': node_data.get('config', {}).get('meta',{}).get('nr_config',{}).get('message',''),
        })
    return fields

def get_dbt_cloud_manifest(run_id: str, http_conn_id: str) -> dict:
    http_hook = HttpHook(http_conn_id=http_conn_id, method='GET')
    endpoint = f'/runs/{run_id}/artifacts/manifest.json'
    token = http_hook.get_connection(http_conn_id).password
    headers = {
        'Content-Type': "application/json",
        'Authorization': f"Token {token}"
    }

    try:
        
        response = http_hook.run(endpoint=endpoint,headers=headers)
        response.raise_for_status()
        return response.json()

    except Exception as e:
        # Some jobs do not have a manifest. if the dbt command failed
        print(f'Could not retrieve manifest.json from dbt cloud for run_id: {run_id}. Exception: {e}')
        return {}


def get_dbt_cloud_run_results(dbt_job_id: str, 
                              dbt_run_id: str, 
                              http_conn_id: str,
                              query_list) -> dict:
    # query_list = read_config('dbt_discovery_queries.yml')
    json_data_dict ={}
    for query in query_list:
        query_body = f"""
                    query dbtObjects($jobId: Int!, $runId: Int) {{
                    {query['query']}   
                    }}"""
        variables = {
                "jobId": int(dbt_job_id),
                "runId": int(dbt_run_id)
            }
        

        http_hook = HttpHook(method='POST', http_conn_id=http_conn_id)
        api_key = http_hook.get_connection(http_conn_id).password
        headers = {
        'Authorization': f"Token {api_key}",
        'Content-Type': 'application/json'
        }
        response = http_hook.run(json={"query": query_body, "variables": variables}, headers=headers) 

        if response.status_code == 200  and dbt_cloud_validation([response]) and query['resource_type'] in response.json()['data']:
            json_data_dict.update(response.json()['data'])
        else:
            raise Exception('Dbt cloud Discovery API returned invalid data')

    return json_data_dict