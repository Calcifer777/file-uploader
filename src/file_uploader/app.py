import asyncio
import os
from pathlib import Path
import time
from typing import Annotated, Any, List, Dict
import uuid

import aiofiles
from azure.storage.blob import BlobBlock, BlobServiceClient, ContainerClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from fastapi import FastAPI, Form, UploadFile, File
import json
from pydantic import Json
import smart_open

PATH_OUTPUT = Path("./output/")
PATH_OUTPUT.mkdir(exist_ok=True, parents=True)


app = FastAPI()


@app.post("/files")
async def get_file(file: bytes = File(...)):
    print(f"Received a {len(file)/2**20:.0f}Mb file!")
    time.sleep(5)


# file.file: tempfile.SpooledTemporaryFile
@app.post("/upload-smart")
async def upload_file(file: UploadFile = File(...)):
    """Read whole file in memory, write to target"""
    path = f"output/{file.filename}"
    with smart_open.open(path, "wb") as fp_out:
        fp_out.write(file.file.read())  # type: ignore

    return {
        "file": file.filename,
        "content": file.content_type,
        "path": path,
    }


CHUNK_SIZE = 1024 * 1024  # 1 Mb


# file.file: tempfile.SpooledTemporaryFile
@app.post("/upload-chunks")
async def upload_file_chunks(file: UploadFile = File(...)):
    """Read whole file in memory, write to target"""
    path = f"output/{file.filename}"
    async with aiofiles.open(path, "wb") as fp:
        while chunk := await file.read(CHUNK_SIZE):
            print(f"reading {CHUNK_SIZE // 2**20}Mb")
            await fp.write(chunk)

    return {
        "file": file.filename,
        "content": file.content_type,
        "path": path,
    }


# file.file: tempfile.SpooledTemporaryFile
@app.post("/upload-chunks-smart")
async def upload_file_chunks_smart(file: UploadFile = File(...)):
    """Read whole file in memory, write to target"""
    path = f"output/{file.filename}"
    with smart_open.open(path, "wb") as fp:
        while chunk := await file.read(CHUNK_SIZE):
            time.sleep(1)
            print(f"reading {CHUNK_SIZE // 2**20}Mb")
            fp.write(chunk)  # type: ignore

    return {
        "file": file.filename,
        "content": file.content_type,
        "path": path,
    }


# file.file: tempfile.SpooledTemporaryFile
@app.post("/upload-chunks-smart-with-meta")
async def upload_file_chunks_smart_with_meta(
    file: Annotated[UploadFile | None, File(...)] = None,
    metadata: Annotated[Json, Form(default_factory=dict)] = dict(),
):
    """Read whole file in memory, write to target"""
    if file is not None:
        path = f"output/{file.filename}"
        with smart_open.open(path, "wb") as fp:
            while chunk := await file.read(CHUNK_SIZE):
                print(f"reading {CHUNK_SIZE // 2**20}Mb")
                time.sleep(0.2)
                fp.write(chunk)  # type: ignore
    print("Metadata is: ", metadata)


os.environ["STORAGE_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
AZ_CONTAINER_NAME = "test-container"


blob_service_client = BlobServiceClient.from_connection_string(
    os.environ["STORAGE_CONNECTION_STRING"]
)


containers = blob_service_client.list_containers(name_starts_with=AZ_CONTAINER_NAME)
if len(list(containers)) == 0:
    blob_service_client.create_container(AZ_CONTAINER_NAME)


@app.post("/upload-azure-chunks-with-meta")
async def upload_azure_file_chunks_smart_with_meta(
    file: Annotated[UploadFile | None, File(...)] = None,
    metadata: Annotated[Json, Form(default_factory=dict)] = dict(),
):
    """Read whole file in memory, write to azure"""
    if file is not None and file.filename is not None:
        blob_client = blob_service_client.get_blob_client(
            container=AZ_CONTAINER_NAME,
            blob=file.filename,
        )
        block_list = []
        while chunk := await file.read(CHUNK_SIZE):
            print(f"reading {CHUNK_SIZE // 2**20}Mb")
            block_id = str(uuid.uuid4())
            blob_client.stage_block(block_id=block_id, data=chunk)  # type: ignore
            block_list.append(BlobBlock(block_id=block_id))
        blob_client.commit_block_list(block_list=block_list)
    print("Metadata is: ", metadata)


async_blob_service_client = AsyncBlobServiceClient.from_connection_string(
    os.environ["STORAGE_CONNECTION_STRING"]
)


# does it even make sense?
# raises `Unclosed client session` at shutdown, is it an issue?
@app.post("/upload-azure-chunks-with-meta-async")
async def upload_azure_file_chunks_smart_with_meta_async(
    file: Annotated[UploadFile | None, File(...)] = None,
    metadata: Annotated[Json, Form(default_factory=dict)] = dict(),
):
    """Read whole file in memory, write to azure"""
    if file is not None and file.filename is not None:
        blob_client = async_blob_service_client.get_blob_client(
            container=AZ_CONTAINER_NAME,
            blob=file.filename,
        )
        block_list = []
        tasks = []
        while chunk := await file.read(CHUNK_SIZE):
            print(f"reading {CHUNK_SIZE // 2**20}Mb")
            block_id = str(uuid.uuid4())
            op = blob_client.stage_block(
                block_id=block_id,
                data=chunk,  # type: ignore
            )
            tasks.append(op)
            block_list.append(BlobBlock(block_id=block_id))
        await asyncio.gather(*tasks)
        await blob_client.commit_block_list(block_list=block_list)
    print("Metadata is: ", metadata)
