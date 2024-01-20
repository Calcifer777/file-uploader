#!/bin/bash

curl -X 'POST' \
  'http://localhost:8000/upload-chunks-smart-with-meta' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@files/100mb.dat' \
  -F 'metadata={"key": 1}'
