import asyncio
import inspect
import logging
from airflow.providers.http.hooks.http import HttpAsyncHook

log = logging.getLogger(__name__)


async def run_in_loop(hook, data, headers):
    """Run a single async HTTP request using the provided hook.

    This inspects the hook.run signature to decide whether to send the payload
    as `json=` or `data=` so it works across provider versions.
    Returns a tuple of (status, response_text).
    """
    try:
        sig = inspect.signature(hook.run)
        params = sig.parameters
        kwargs = {"headers": headers, "extra_options": {"compress": True}}
        if 'json' in params:
            kwargs['json'] = data
        else:
            kwargs['data'] = data

        response = await hook.run(**kwargs)
        # aiohttp-like responses have .status and .text()
        status = getattr(response, 'status', None) or getattr(response, 'status_code', None)
        text = await response.text()
        return status, text
    except Exception as exc:
        log.exception('Error running async upload: %s', exc)
        raise


def upload_data(records: list, http_conn_id: str, chunk_size=100) -> None:
    """Upload records to New Relic in chunks using an async HTTP hook.

    Args:
        records: list of record dicts to send. Each record should contain an "eventType".
        http_conn_id: Airflow HTTP connection id to use for the POST requests.
        chunk_size: number of records per request.
    """
    if not records:
        log.debug('No records to upload')
        return

    hook = HttpAsyncHook(method='POST', http_conn_id=http_conn_id)
    # Use the same connection id provided to the function to retrieve the API key.
    try:
        conn = hook.get_connection(http_conn_id)
    except Exception:
        # Fallback to BaseHook behavior if necessary
        conn = hook.get_connection(http_conn_id)

    api_key = getattr(conn, 'password', None)
    if not api_key:
        log.error('No API key found in connection %s', http_conn_id)
        raise RuntimeError(f'No API key found in connection {http_conn_id}')

    headers = {"Api-Key": api_key, "Content-Type": "application/json"}

    tasks = []
    for chunk_start in range(0, len(records), chunk_size):
        chunk_records = records[chunk_start:chunk_start + chunk_size]
        try:
            event_type = chunk_records[0].get('eventType') if chunk_records else 'unknown'
        except Exception:
            event_type = 'unknown'
        log.info('Sending chunk %d..%d (%d records) for eventType: %s',
                 chunk_start, chunk_start + len(chunk_records) - 1, len(chunk_records), event_type)
        tasks.append(run_in_loop(hook, chunk_records, headers))

    # Ensure we have an event loop available. Create one if needed.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # If the loop is already running (rare in Airflow workers), run tasks synchronously
    if loop.is_running():
        # Fall back to running each task sequentially via asyncio.run in a new loop
        results = []
        for t in tasks:
            try:
                results.append(asyncio.run(t))
            except Exception as exc:
                log.exception('Async chunk upload failed: %s', exc)
                results.append((None, str(exc)))
    else:
        results = loop.run_until_complete(asyncio.gather(*tasks))

    # Log results summary
    for status, body in results:
        log.info('NR upload result status=%s body=%s', status, (body[:200] + '...') if isinstance(body, str) and len(body) > 200 else body)