package outboxer

import (
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gitlotto/common/env_var"
	"github.com/gitlotto/common/logging"
)

const amountOfWorkflowsToOutbox = 100
const nextStartIn = time.Minute * 10

func Run() {

	logger := logging.MustCreateZuluTimeLogger()
	defer logger.Sync()

	envVarReader := env_var.EnvVarReader{
		Logger: logger,
	}

	workflowsTableName := envVarReader.MustFind("WORKFLOWS_TABLE_NAME")
	openWorkflowsIndexName := envVarReader.MustFind("OPEN_WORKFLOWS_INDEX_NAME")
	notificationTopicArn := envVarReader.MustFind("NOTIFICATION_TOPIC_ARN")
	awsRegion := envVarReader.MustFind("AWS_REGION")

	awsConfig := &aws.Config{
		Region: &awsRegion,
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		logger.Error("impossible to create an AWS session!")
		panic(err)
	}

	outboxer := Outboxer{
		workflowsTableName:        workflowsTableName,
		openWorkflowsIndexName:    openWorkflowsIndexName,
		notificationTopicArn:      notificationTopicArn,
		amountOfWorkflowsToOutbox: amountOfWorkflowsToOutbox,
		nextStartIn:               nextStartIn,
		logger:                    logger,
		awsSession:                awsSession,
	}

	handler := func(event events.CloudWatchEvent) error {
		return outboxer.Outbox(event.ID)
	}

	lambda.Start(handler)

}
