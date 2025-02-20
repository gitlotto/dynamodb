module github.com/gitlotto/common/direct_pass

go 1.23.6

require (
	github.com/aws/aws-sdk-go v1.51.30
	github.com/google/uuid v1.6.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
)

require (
	github.com/gitlotto/common/zulu v0.10.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require github.com/gitlotto/common/database v0.0.0-00010101000000-000000000000

require (
	github.com/aws/aws-lambda-go v1.47.0
	github.com/gitlotto/common/env_var v0.0.0-00010101000000-000000000000
	github.com/gitlotto/common/logging v0.0.0-00010101000000-000000000000
	github.com/gitlotto/common/notification v0.0.0-00010101000000-000000000000
	github.com/gitlotto/common/queue v0.0.0-00010101000000-000000000000
	github.com/gitlotto/common/workflows v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.27.0
)


replace github.com/gitlotto/common/workflows => ../workflows

replace github.com/gitlotto/common/database => ../database

replace github.com/gitlotto/common/env_var => ../env_var

replace github.com/gitlotto/common/logging => ../logging

replace github.com/gitlotto/common/notification => ../notification

replace github.com/gitlotto/common/queue => ../queue

