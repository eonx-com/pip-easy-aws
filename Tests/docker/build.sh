#!/usr/bin/env bash

echo
echo Building SFTP test server...
echo
docker build . -t "easy-lambda-sftp"
echo
