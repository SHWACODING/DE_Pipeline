#!/bin/bash
docker compose down -v

sleep 5

cd airbyte || exit

docker compose down