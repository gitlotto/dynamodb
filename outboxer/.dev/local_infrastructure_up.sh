#! /bin/sh

samlocal deploy --template-file .dev/db.yaml --stack-name outboxer_dynamodb --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=outboxer_dynamodb
samlocal deploy --template-file .dev/random_queues.yaml --stack-name outboxer_random_queues --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=outboxer_random_queues
samlocal deploy --template-file .dev/notification.yaml --stack-name outboxer_notification --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=outboxer_notification
