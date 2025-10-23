
import pendulum
import os
import uuid
import requests
from airflow.decorators import dag, task
from airflow.models import XCom, Variable
from airflow.utils.db import create_session
from airflow.providers.http.operators.http import HttpOperator
from airflow.hooks.base import BaseHook
# Import utility functions
from nr_utils.snowflake import get_failed_test_rows
from nr_utils.nr_utils import (
    flatten_dict,
    get_team_from_run,
    extract_time_components,
    read_config
    
)
from nr_utils.dbt_cloud import (
    dbt_cloud_response_filter,
    dbt_cloud_secure_response_filter,
    dbt_cloud_validation,
    paginate_dbt_cloud_api_response,
    get_dbt_cloud_manifest,
    get_dbt_cloud_manifest_filtered,
    get_dbt_cloud_run_results,
)
from nr_utils.http import upload_data


current_directory = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_directory,'dag_config.yml')
config = read_config(file_path)
connections = config['connections']
dbt_cloud_admin_api = connections['dbt_cloud_admin_api']
nr_insights_query = connections['nr_insights_query'] 
dbt_cloud_discovery_api =  connections['dbt_cloud_discovery_api'] 
nr_insights_insert = connections['nr_insights_insert']
snowflake_api =  connections['snowflake_api']
default_team = config['default_team']

NR_ACCOUNT_ID = int(Variable.get('nr_account_id'))

NERDGRAPH_NRQL_QUERY = """
query($accountId: Int!, $nrql: Nrql!) {
  actor {
    account(id: $accountId) {
      nrql(query: $nrql) {
        results
      }
    }
  }
}
"""

NERDGRAPH_ENDPOINT = "https://api.newrelic.com/graphql"


def execute_nerdgraph_nrql_query(nrql_query):
    conn = BaseHook.get_connection(nr_insights_query)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'API-Key': conn.password,
    }
    payload = {
        'query': NERDGRAPH_NRQL_QUERY,
        'variables': {
            'accountId': NR_ACCOUNT_ID,
            'nrql': nrql_query,
        },
    }
    response = requests.post(NERDGRAPH_ENDPOINT, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return nerdgraph_members_response_filter(response)


def nerdgraph_members_response_filter(response):
    payload = response.json()
    errors = payload.get('errors')
    if errors:
        raise ValueError(f"NerdGraph returned errors: {errors}")
    account_data = (
        payload.get('data', {})
        .get('actor', {})
        .get('account', {})
    )
    nrql_data = account_data.get('nrql', {})
    results = nrql_data.get('results', [])
    if results and isinstance(results[0], dict):
        return results[0].get('members', [])
    return []


@dag(
    # Set start_date and catchup=True to get historical data
    start_date=pendulum.datetime(2024, 6, 10, tz="UTC"),
    catchup=False,
    tags=[],
    max_active_runs=3,
    schedule="5,15,25,35,45,55 * * * *",
    default_args={
        'retries': 3,
        'retry_delay': pendulum.duration(seconds=30),
    }
)


def new_relic_data_pipeline_observability_get_dbt_run_metadata2():

    get_dbt_runs = HttpOperator(
        task_id='get_dbt_runs',
        http_conn_id=dbt_cloud_admin_api,
        method='GET',
        endpoint='/runs/',
        headers={
            'Content-Type': "application/json",
            'Authorization': 'Token {{conn.dbt_cloud_admin_api.password}}',
        },
        data={
            # Shift the interval five minutes to account for API latency
            'finished_at__range': '["{{data_interval_start.subtract(minutes=5)}}", "{{data_interval_end.subtract(seconds=0.000001, minutes=5)}}"]',
            'include_related': ['job'],
            'order_by': '-finished_at',
        },
        do_xcom_push=True,
        pagination_function=paginate_dbt_cloud_api_response,
        response_check=dbt_cloud_validation,
        response_filter=dbt_cloud_response_filter
    )
 

    get_dbt_projects = HttpOperator(
        task_id='get_dbt_projects',
        http_conn_id=dbt_cloud_admin_api,
        method='GET',
        endpoint='/projects/',
        headers={
            'Content-Type': "application/json",
            'Authorization': 'Token {{conn.dbt_cloud_admin_api.password}}',
        },
        do_xcom_push=True,
        pagination_function=paginate_dbt_cloud_api_response,
        response_check=dbt_cloud_validation,
        response_filter=dbt_cloud_secure_response_filter,
    )


    # Uses a different response filter because the environment can hold sensative data
    get_dbt_environments = HttpOperator(
        task_id='get_dbt_environments',
        http_conn_id=dbt_cloud_admin_api,
        method='GET',
        endpoint='/environments/',
        headers={
            'Content-Type': "application/json",
            'Authorization': 'Token {{conn.dbt_cloud_admin_api.password}}',
        },
        do_xcom_push=True,
        pagination_function=paginate_dbt_cloud_api_response,
        response_check=dbt_cloud_validation,
        response_filter=dbt_cloud_secure_response_filter
    )


    # Get run ids already in NR1. This improves idempotency
    @task(multiple_outputs=True)
    def get_nrql_queries(runs, data_interval_start=None):
        if len(runs) > 200:
            print(f'Too many runs to process. Ensure the DAG has a schedule or decrease the scheduled interval')
            raise Exception('Too many runs to process')

        run_ids = [run['run_id'] for run in runs]

        run_query = f"""
            select uniques(run_id) from dbt_job_run
            where run_id in ('{"', '".join(run_ids)}')
            since '{data_interval_start.format("YYYY-MM-DD HH:mm:ss")}'
        """
        resource_run_query = f"""
            select uniques(run_id) from dbt_resource_run
            where run_id in ('{"', '".join(run_ids)}')
            since '{data_interval_start.format("YYYY-MM-DD HH:mm:ss")}'
        """
        failed_test_row_query = f"""
            select uniques(run_id) from dbt_failed_test_rows
            where run_id in ('{"', '".join(run_ids)}')
            since '{data_interval_start.format("YYYY-MM-DD HH:mm:ss")}'
        """
        queries = {
            'run_query': run_query,
            'resource_run_query': resource_run_query,
            'failed_test_row_query': failed_test_row_query
        }
        print(f'NR Run id query: {queries}')
        return queries 
    

    @task
    def get_nr_run_ids(nrql_query):
        return execute_nerdgraph_nrql_query(nrql_query)
 

    @task
    def get_nr_resource_run_ids(nrql_query):
        return execute_nerdgraph_nrql_query(nrql_query)


    @task
    def get_nr_failed_test_row_run_ids(nrql_query):
        return execute_nerdgraph_nrql_query(nrql_query)


    # Compare runs from dbt cloud to run ids already in New Relic
    @task(multiple_outputs=True)
    def get_runs_to_process(runs, nr_runs, nr_resource_runs, nr_failed_test_runs):
        runs_to_process = list(filter(lambda run: run['run_id'] not in nr_runs, runs))
        resource_runs_to_process = list(filter(lambda run: run['run_id'] not in nr_resource_runs, runs))
        failed_test_ids_to_process = [run['run_id'] for run in runs if run not in nr_failed_test_runs]
        result = {
            'runs_to_process': runs_to_process,
            'resource_runs_to_process': resource_runs_to_process,
            'failed_test_runs_to_process': failed_test_ids_to_process
        }
        return result


    @task 
    def enrich_runs(runs_to_process, projects, environments):
        processed_runs = []
        for raw_run in runs_to_process:
            # Add job information 
            job = flatten_dict(raw_run['job'], 'job_')
            run = flatten_dict(raw_run, 'run_')
            run.update(job)
            del(run['run_job'])

            # Add in environment
            environment = environments[run['run_environment_id']]
            environment_flat = flatten_dict(environment, 'environment_')
            run.update(environment_flat)
            del(run['run_environment_id'])
            
            # Add in project
            project = projects[run['run_project_id']]
            project_flat = flatten_dict(project, 'project_') 
            run.update(project_flat)
            del(run['run_project_id'])

            # Add in attributes for entities
            run['entity_name'] = f"{run['job_name']} - {run['run_created_at']}"
            run['entity_id'] = f"{uuid.uuid4()}"
            run['dbt_source'] = 'Dbt Cloud'

            # Add event type
            run['eventType'] = 'dbt_job_run'

            # Set team
            run['run_team'] = get_team_from_run(run)

            # get duration
            run['run_total_seconds'] = extract_time_components(run)

            processed_runs.append(run)

        return processed_runs


    @task 
    def process_runs(runs):
        if runs:
            print(f'Sending {len(runs)} to New Relic') 
            print(f'Run ids: {[run["run_id"] for run in runs]}')
            upload_data(runs, nr_insights_insert, chunk_size=500)
        else:
            print('No new runs to send')
        print('Send run complete')


    @task(multiple_outputs=True)
    def process_resource_runs(runs, failed_test_runs):
        # Used to collect failed test that need failed test row processing
        all_failed_tests = []
        for run in runs:
            run_id = run['run_id']
            job_id = run['job_id']
            # Get run metadata
            if run['run_status'] not in (10, 20):
                print(f'Run {run["run_id"]} did not complete. Not getting models and tests')
                continue 

            # Manifest contains all resources even if they were not run. This is how we can get the state of the project.
            manifest_raw = get_dbt_cloud_manifest(run_id, dbt_cloud_admin_api)
            
            manifest_filtered = get_dbt_cloud_manifest_filtered(manifest_raw)

            manifest = {resource['unique_id']: resource for resource in manifest_filtered}

            # Get run statuses 
            dbt_query_path = os.path.join(current_directory,'dbt_discovery_queries.yml')
            query_list = read_config(dbt_query_path)
            
            status_dict = get_dbt_cloud_run_results(job_id, run_id, dbt_cloud_discovery_api, query_list)
            statuses = status_dict['models'] + status_dict['snapshots'] + status_dict['seeds'] + status_dict['tests']

            resource_run_statuses = []

            for status in statuses:
                if status['unique_id'] not in manifest: # check in case some runs don't have a manifest file
                    print(f"key not found error: '{status['unique_id']}' not found in manifest for run_id: {run_id}")
                    continue
                resource_metadata = manifest[status['unique_id']]
                status.update(resource_metadata)
                status.update(run)
                status['eventType'] = 'dbt_resource_run'
                status['entity_name'] = f'{status["alias"]} - {status["run_created_at"]}'
                status['entity_id'] = f'{uuid.uuid4()}' 
                status['dbt_source'] = 'Dbt Cloud'
                # Save failed tests that need failed test row processing
                if status['status'] in ('warn', 'fail') and status['alert_failed_test_rows']:
                    all_failed_tests.append(status.copy())
                resource_run_statuses.append(flatten_dict(status, ''))

            if resource_run_statuses:
                print(f'Sending {len(resource_run_statuses)} resource runs') 
                upload_data(resource_run_statuses, nr_insights_insert, chunk_size=500)
                print(f'Send complete')

        print(f'Finished processing {len(runs)} resource runs')
        return {
            'failed_tests': all_failed_tests,
            'failed_test_runs': failed_test_runs,
        }


    @task 
    def process_failed_test_rows(failed_tests, failed_test_runs):
        if failed_tests and failed_test_runs:
            # See if we already processed the failed tests
            failed_tests_to_process = [test for test in failed_tests if test['run_id'] in failed_test_runs]
            failed_test_rows = get_failed_test_rows(failed_tests_to_process, snowflake_conn_id=snowflake_api)
            # Send data to NR1
            print(f'Sending {len(failed_test_rows)} failed test rows')
            upload_data(failed_test_rows, nr_insights_insert, chunk_size=500)
        else:
            print('No failed tests to get failed test rows for') 

        print(f'Send failed test rows complete')
        return 'Process failed test rows complete' 


    @task
    def cleanup_xcom(message=None, **kwargs):
        dag_id = kwargs["ti"].dag_id
        run_id = kwargs["run_id"]

        print(f'Deleting Xcoms for this dagrun')
        with create_session() as session:
            session.query(XCom).filter(XCom.dag_id == dag_id, XCom.run_id == run_id).delete()
        print(f'Dag Xcoms deleted')

    # Using a combination of taskflow and operators. Task flow automatically handles task dependencies in the DAG
    # Get run data 
    dbt_projects = get_dbt_projects.output['return_value']
    dbt_environments = get_dbt_environments.output['return_value']
    dbt_runs = get_dbt_runs.output['return_value']
    dbt_runs_enriched = enrich_runs(dbt_runs, dbt_projects, dbt_environments)

    # Get run ids to proces for runs, resource runs, and failed test runs
    nr_run_queries = get_nrql_queries(dbt_runs_enriched)
    nr_runs = get_nr_run_ids(nr_run_queries['run_query'])
    nr_resource_runs = get_nr_resource_run_ids(nr_run_queries['resource_run_query'])
    nr_failed_test_row_runs = get_nr_failed_test_row_run_ids(nr_run_queries['failed_test_row_query'])
    runs_to_process = get_runs_to_process(dbt_runs_enriched, nr_runs, nr_resource_runs, nr_failed_test_row_runs)

    # Process runs
    processed_runs = process_runs(runs_to_process['runs_to_process'])

    failed_tests = process_resource_runs(
        runs_to_process['resource_runs_to_process'],
        runs_to_process['failed_test_runs_to_process'])

    failed_test_rows = process_failed_test_rows(
        failed_tests['failed_tests'], 
        failed_tests['failed_test_runs'])

    # Cleanup xcoms
    cleanup_xcom(failed_test_rows)

new_relic_data_pipeline_observability_get_dbt_run_metadata2()
