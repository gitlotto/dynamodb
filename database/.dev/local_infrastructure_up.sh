#! /bin/sh

samlocal deploy --template-file .dev/db.yaml --stack-name database --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --s3-bucket database --parameter-overrides TheStackName=gitlotto
