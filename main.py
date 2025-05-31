#!/usr/bin/env python3
import os
import sys
import json
import time
from datetime import datetime
from functools import partial
import typing

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.concurrency import run_in_threadpool
from starlette.background import BackgroundTask
from starlette.types import Send, Receive, Scope

from log import OpenAILog, save_log, LogQuery
from utils import PathMatchingTree, OverrideStreamResponse

proxied_hosts = PathMatchingTree({
    "/": os.environ.get("OPENAI_PROXY_UNDERLYING", "https://api.openai.com"),
    "/backend-api/conversation": os.environ.get("OPENAI_PROXY_BACKEND_API_CONVERSATION", "https://chat.openai.com"),
})
if os.environ.get("OPENAI_PROXY_DEBUG", "0") == "1":
    def _debug_print(arg):
        sys.stderr.write(arg+'\n')
else:
    def _debug_print(arg):
        pass
timout_minutes = 23.0


# FastAPI app
app = FastAPI()


async def proxy_openai_api(request: Request):
    # proxy request to OpenAI API
    headers = {k: v for k, v in request.headers.items() if
               k not in {'host', 'content-length', 'x-forwarded-for', 'x-real-ip', 'connection'}}
    url = f'{proxied_hosts.get_matching(request.url.path)}{request.url.path}'

    start_time = int(time.time() * 1000)  # Current time in milliseconds
    # create httpx async client with proper timeout and retry configuration
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(60*timout_minutes, connect=5.0),
        transport=httpx.AsyncHTTPTransport(retries=2)
    )

    request_body_bytes = await request.body()
    request_body_for_log = None
    request_body_for_upstream = None

    if request.method in {'POST', 'PUT'}:
        try:
            # For logging, decode assuming UTF-8.
            request_body_for_log = request_body_bytes.decode('utf-8')
            # For upstream, parse as JSON. If this fails, it's a client error (400).
            # However, some OpenAI endpoints might expect non-JSON POSTs (e.g. file uploads).
            # For now, we assume JSON if Content-Type suggests it.
            content_type = request.headers.get("content-type", "").lower()
            if "application/json" in content_type:
                request_body_for_upstream = json.loads(request_body_for_log)
            else:
                # If not JSON, pass bytes directly (or handle other types)
                # For this example, we'll assume it's not common for OpenAI and stick to JSON or None
                # If you need to support other POST types, this logic needs expansion.
                request_body_for_upstream = None # Or pass request_body_bytes if upstream expects raw bytes
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        except UnicodeDecodeError:
            # If body isn't UTF-8, log it as undecodable or hex representation
            request_body_for_log = f"<undecodable_binary_body_length_{len(request_body_bytes)}>"
            # And assume it's not JSON for upstream if it couldn't be decoded as text.
            request_body_for_upstream = None # Or handle as appropriate

    # Create and populate log entry early
    log = OpenAILog(
        request_url=url,
        request_method=request.method,
        request_time=start_time,
        request_content=request_body_for_log
    )

    async def stream_api_response():
        nonlocal log # Ensures we are updating the log object from the outer scope
        try:
            st = client.stream(
                request.method,
                url,
                headers=headers,
                params=request.query_params,
                json=request_body_for_upstream, # Use parsed JSON body for upstream
                # If supporting non-JSON POSTs, use `content=request_body_bytes` instead of `json`
            )
            async with st as res:
                response.status_code = res.status_code
                response.init_headers({k: v for k, v in res.headers.items() if
                                       k not in {'content-length', 'content-encoding', 'alt-svc'}})

                content = bytearray()
                async for chunk in res.aiter_bytes():
                    yield chunk
                    content.extend(chunk)

                # Update log with response data for success case
                log.response_time = int((time.time() * 1000) - start_time)
                log.status_code = res.status_code
                log.response_content = content.decode('utf-8', errors='replace') # Handle potential decode errors
                log.response_header = json.dumps([[k, v] for k, v in res.headers.items()])

        except httpx.ReadTimeout as exc:
            log.status_code = 504
            log.response_content = "Upstream service timed out"
            log.response_time = int((time.time() * 1000) - start_time)
            # Log saving will be handled by the background task
            raise HTTPException(status_code=504, detail="Upstream service timed out")
        except httpx.RequestError as exc: # Covers ConnectError, etc.
            log.status_code = 502
            log.response_content = f"Bad gateway error: {str(exc)}"
            log.response_time = int((time.time() * 1000) - start_time)
            # Log saving will be handled by the background task
            raise HTTPException(
                status_code=502,
                detail=f"Bad gateway error: {str(exc)}"
            )
        # Other exceptions will propagate and be handled by FastAPI's default error handling,
        # or by OverrideStreamResponse's __call__ method's exception handling.
        # The log for these unhandled cases might be incomplete but will be saved by update_log.

    async def update_log():
        nonlocal log, start_time # Ensure access to the correct 'log' and 'start_time'
        try:
            # If response_time or status_code wasn't set due to an unexpected error
            # before normal completion or handled exception in stream_api_response.
            if log.response_time is None:
                log.response_time = int((time.time() * 1000) - start_time)

            # If status_code is still None here, it means an error occurred very early
            # or in an unhandled way. It will be saved as NULL in the DB if not set.
            # For example, if client disconnects before stream_api_response really starts.
            if log.status_code is None:
                # Potentially set a default error status if none is available
                # For now, we let it be None, as the DB column is nullable.
                print(f"⚠️ Log for {log.request_url} has no status_code at save time.")


            await save_log(log)
            # The print from save_log in log.py will indicate DB operation status.
            # This print confirms the background task ran.
            print(f"ℹ️ Background task processed log for {log.request_url} (Status: {log.status_code})")
        except Exception as e:
            # This catches errors from save_log itself or other issues within update_log
            print(f"❌ Failed to save log via background task: {e}. Log URL: {log.request_url}, Status: {log.status_code}")

    response = OverrideStreamResponse(stream_api_response(), background=BackgroundTask(update_log))
    return response


@app.get("/lastlog")
async def get_last_log():
    logs = await run_in_threadpool(LogQuery.get_all, 1)
    if not logs:
        raise HTTPException(status_code=404, detail="No logs found")
    return logs[0].to_dict()


@app.route('/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
async def request_handler(request: Request):
    return await proxy_openai_api(request)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info", reload=True)
