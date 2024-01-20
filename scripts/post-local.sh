#!/bin/bash

curl -X 'POST' \
  'http://localhost:8000/upload-azure-chunks-with-meta-async' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@files/10mb.dat' \
  -F 'metadata={"key": 1}'
