In order to run integration tests run
```docker compose -f ./.dev/docker-compose_local.yaml --env-file ./.dev/.env up```,
then
```aws --endpoint-url http://localhost:4566  s3api create-bucket --bucket gitlotto```,
then


[//]: # (```samlocal deploy --template-file template_resources.yaml --stack-name chessfinder --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket chessfinder```,)
```samlocal deploy --template-file .infrastructure/db.yaml --stack-name gitlotto_dynamodb --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=gitlotto_dynamodb```,
```samlocal deploy --template-file .infrastructure/notification.yaml --stack-name gitlotto_notification --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=gitlotto_notification```,
```samlocal deploy --template-file .infrastructure/queue.yaml --stack-name gitlotto_sqs --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=gitlotto_sqs```
