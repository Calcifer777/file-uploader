from pathlib import Path
import time

import aiofiles
from fastapi import FastAPI, UploadFile, File
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
async def upload_file(file: UploadFile = File(...)):
    """Read whole file in memory, write to target"""
    path = f"output/{file.filename}"
    with smart_open.open(path, "wb") as fp:
        while chunk := await file.read(CHUNK_SIZE):
            print(f"reading {CHUNK_SIZE // 2**20}Mb")
            fp.write(chunk)  # type: ignore

    return {
        "file": file.filename,
        "content": file.content_type,
        "path": path,
    }