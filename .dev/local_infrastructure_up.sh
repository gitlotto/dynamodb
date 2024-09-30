#! /bin/sh

docker compose -f ./.dev/docker-compose_local.yaml --env-file ./.dev/.env -p gitlotto up

aws --endpoint-url http://localhost:4566  s3api create-bucket --bucket gitlotto 
samlocal deploy --template-file .dev/db.yaml --stack-name gitlotto --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=gitlotto
