#!/usr/bin/env python3
import os
import json
import time
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, HTTPException
from starlette.background import BackgroundTask

from log import OpenAILog, save_log
from utils import PathMatchingTree, OverrideStreamResponse

proxied_hosts = PathMatchingTree({
    "/": os.environ.get("OPENAI_PROXY_UNDERLYING", "https://api.openai.com"),
    "/backend-api/conversation": os.environ.get("OPENAI_PROXY_BACKEND_API_CONVERSATION", "https://chat.openai.com"),
})

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
        timeout=httpx.Timeout(30.0, connect=5.0),
        transport=httpx.AsyncHTTPTransport(retries=2)
    )

    request_body = await request.json() if request.method in {'POST', 'PUT'} else None

    # Create and populate log entry early
    log = OpenAILog(
        request_url=url,
        request_method=request.method,
        request_time=start_time,
        request_content=(await request.body()).decode('utf-8') if request.method == 'POST' else None
    )

    async def stream_api_response():
        nonlocal log
        try:
            st = client.stream(request.method, url, headers=headers, params=request.query_params, json=request_body)
            async with st as res:
                response.status_code = res.status_code
                response.init_headers({k: v for k, v in res.headers.items() if
                                       k not in {'content-length', 'content-encoding', 'alt-svc'}})

                content = bytearray()
                async for chunk in res.aiter_bytes():
                    yield chunk
                    content.extend(chunk)

                # Update log with response data
                log.response_time = int((time.time() * 1000) - start_time)
                log.status_code = res.status_code
                log.response_content = content.decode('utf-8')
                log.response_header = json.dumps([[k, v] for k, v in res.headers.items()])

        except httpx.ReadTimeout as exc:
            log.status_code = 504
            log.response_content = "Upstream service timed out"
            log.response_time = int((time.time() * 1000) - start_time)
            try:
                await save_log(log)
                print(f"✅ Saved timeout log (504) in {log.response_time}ms")
            except Exception as e:
                print(f"❌ Failed to save timeout log: {e}")
            raise HTTPException(status_code=504, detail="Upstream service timed out")
        except httpx.RequestError as exc:
            log.status_code = 502
            log.response_content = f"Bad gateway error: {str(exc)}"
            log.response_time = int((time.time() * 1000) - start_time)
            try:
                await save_log(log)
                print(f"✅ Saved error log (502) in {log.response_time}ms")
            except Exception as e:
                print(f"❌ Failed to save error log: {e}")
            raise HTTPException(
                status_code=502,
                detail=f"Bad gateway error: {str(exc)}"
            )

    async def update_log():
        nonlocal log
        # Only save if not already saved (error cases save immediately)
        if log.status_code is None:
            log.response_time = int((time.time() * 1000) - start_time)
            try:
                await save_log(log)
                print(f"✅ Saved success log ({log.status_code}) in {log.response_time}ms")
            except Exception as e:
                print(f"❌ Failed to save success log: {e}")

    response = OverrideStreamResponse(stream_api_response(), background=BackgroundTask(update_log))
    return response


@app.route('/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
async def request_handler(request: Request):
    return await proxy_openai_api(request)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info", reload=True)
