import asyncio
from airflow.providers.http.hooks.http import HttpAsyncHook


async def run_in_loop(hook, data, headers):
    # Uploads data to New Relic. Different versions of HttpAsyncHook expect different calls
    # Starting in 4.10.1, data and json are separated. Uncomment the line that matches your
    # environment.
    # http provider 4.10.1+
    response = await hook.run(json=data, headers=headers, extra_options={'compress': True})
    # http provider 4.10.0-
    # response = await hook.run(data=data, headers=headers, extra_options={'compress': True})
    text_response = await response.text()

    return text_response


def upload_data(records: list, http_conn_id: str, chunk_size=100) -> None:
    hook = HttpAsyncHook(method='post', http_conn_id=http_conn_id)
    api_key = hook.get_connection('nr_insights_insert').password
    headers = {"Api-Key": api_key, "Content-Type": "application/json"}

    responses = []

    loop = asyncio.get_event_loop()
    for chunk in range(0, len(records), chunk_size):
        chunk_records = records[chunk:chunk+chunk_size]
        print(f'Sending Chunk {len(chunk_records)} records for eventType: {records[0]["eventType"]}')
        responses.append(run_in_loop(hook, chunk_records, headers)) 

    data = loop.run_until_complete(asyncio.gather(*responses))
    print(f'Responses from NR1 {len(data)} responses: {data}')