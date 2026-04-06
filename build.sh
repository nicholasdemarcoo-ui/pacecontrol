#!/usr/bin/env bash
set -o errexit

curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

pip install -r requirements.txt