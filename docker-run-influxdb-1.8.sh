#!/usr/bin/env bash

DEFAULT_DATABASE="defaultdb"
INFLUXDB_USERNAME="admin"

docker run -d \
  --name=influxdb-1 \
  -p 8086:8086 \
  -e INFLUXDB_DB=${DEFAULT_DATABASE} \
  -e INFLUXDB_ADMIN_USER=${INFLUXDB_USERNAME} \
  -e INFLUXDB_ADMIN_PASSWORD=${INFLUXDB_PASSWORD} \
  -v /home/moon-walker/Docker/influxdb_1:/var/lib/influxdb \
  influxdb:1.8

docker run -d \
  --name=influxdb-2 \
  -p 8087:8086 \
  -e INFLUXDB_DB=${DEFAULT_DATABASE} \
  -e INFLUXDB_ADMIN_USER=${INFLUXDB_USERNAME} \
  -e INFLUXDB_ADMIN_PASSWORD=${INFLUXDB_PASSWORD} \
  -v /home/moon-walker/Docker/influxdb_2:/var/lib/influxdb \
  influxdb:1.8