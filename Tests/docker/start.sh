#!/usr/bin/env bash

echo
echo Starting SFTP test server...
echo
echo Username: sftp
echo Password: sftp
echo
docker run -p 22:22 -d --name "easy-lambda-sftp" "easy-lambda-sftp"
echo
