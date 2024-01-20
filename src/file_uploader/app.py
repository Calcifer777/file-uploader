from pathlib import Path
import time
from typing import Annotated, Any, List, Dict

import aiofiles
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
        fp_out.write(file.file.read())

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
