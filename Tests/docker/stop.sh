#!/usr/bin/env bash

echo
echo Stopping SFTP test server...
echo
docker stop "easy-sftp-server"

echo
echo Removing SFTP test server...
echo
docker rm "easy-sftp-server"
echo
