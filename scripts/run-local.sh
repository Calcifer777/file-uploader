#!/bin/bash

mprof run --include-children uvicorn file_uploader.app:app
