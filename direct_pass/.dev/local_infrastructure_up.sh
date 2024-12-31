#! /bin/sh

samlocal deploy --template-file .dev/db.yaml --stack-name direct_passer_dynamodb --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=direct_passer_dynamodb
samlocal deploy --template-file .dev/queues.yaml --stack-name direct_passer_queues --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=direct_passer_queues
samlocal deploy --template-file .dev/notification.yaml --stack-name direct_passer_notification --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket gitlotto --parameter-overrides TheStackName=direct_passer_notification
