import json

import httpx

from file_uploader.app import app

import pytest


FILES = {"file": b"hello, world"}
DATA = {"metadata": json.dumps({"key": "value"})}
URL = "upload-chunks-smart-with-meta"


@pytest.mark.anyio
async def test_upload_file_only():
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        rsp = await client.post(url=URL, files=FILES)
    assert rsp.status_code == 200


@pytest.mark.anyio
async def test_upload_meta_only():
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        rsp = await client.post(url=URL, data=DATA)
    assert rsp.status_code == 200


@pytest.mark.anyio
async def test_upload_meta_and_file():
    async with httpx.AsyncClient(app=app, base_url="http://test") as client:
        rsp = await client.post(url=URL, data=DATA, files=FILES)
    assert rsp.status_code == 200
