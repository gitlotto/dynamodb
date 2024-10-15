#! /bin/sh

docker compose -f ./.dev/docker-compose_local.yaml --env-file ./.dev/.env -p gitlotto up

aws --endpoint-url http://localhost:4566  s3api create-bucket --bucket gitlotto 
