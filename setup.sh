#!/usr/bin/env bash

cd "$(dirname "$0")"
npm install serverless-python-requirements --save-dev
npm install serverless-deployment-bucket --save-dev
rm -rf ./venv
python3 -m venv ./venv
source ./venv/bin/activate
pip install -r ./requirements.txt

