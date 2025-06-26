#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-simbox-with-rules/scripts

sqlcmd --servers=$1 < ../ddl/create_db.sql

$HOME/bin/reload_dashboards.sh simbox.json


