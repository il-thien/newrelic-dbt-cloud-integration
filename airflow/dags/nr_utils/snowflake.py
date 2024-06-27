import uuid
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from nr_utils.nr_utils import flatten_dict
import time
import os 


def get_failed_test_rows(failed_tests: list, snowflake_conn_id: str, max_retries: int = 3, retry_delay: int = 10) -> list:
    # Queries Snowflake with a failed test query
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    failed_test_rows = []
    for test in failed_tests:
        attempt = 0
        success = False
        while attempt < max_retries and not success:
            try:
                # Using the conn directly to avoid logging each row 
                conn = hook.get_conn()
                sql = test['compiled_sql']
                failed_test_row_num = test['failed_test_row_limit']
                print(f'Running sql for failed test {test["unique_id"]}: {sql}')
                cursor = conn.cursor()
                cursor.execute(sql)
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchmany(failed_test_row_num)
                cursor.close()
                conn.close()
                for row in results:
                    failed_row = {}
                    # Create one attribute for each of the first 10 returned columns
                    for index, column in enumerate(columns[0:10]):
                        failed_row[f'field_{index + 1}'] = f'{column}: {row[index]}'
                    failed_row.update(test)
                    failed_row['eventType'] = 'dbt_failed_test_row'
                    failed_row['entity_id'] = f'{uuid.uuid4()}'
                    failed_row['entity_name'] = f'{test["alias"]} - {test["run_created_at"]}'
                    failed_test_rows.append(flatten_dict(failed_row, ''))
                success = True
            except Exception as e:
                attempt += 1
                print(f"Error fetching failed test row on attempt {attempt}/{max_retries}: {str(e)}")
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max retries reached")
                    # Too many things can prevent the query from running. We do not
                    # want to fail the job for failed test rows. 
                    test['field_1'] = 'test_sql_error = ' + str(e)
                    test['eventType'] = 'dbt_failed_test_row'
                    return [flatten_dict(test, '')]
    
    return failed_test_rows