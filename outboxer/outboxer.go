package outboxer

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/notification"
	"github.com/gitlotto/common/workflows"
	"github.com/gitlotto/common/zulu"
	"go.uber.org/zap"
)

type Outboxer struct {
	workflowsTableName        string
	openWorkflowsIndexName    string
	notificationTopicArn      string
	amountOfWorkflowsToOutbox int
	nextStartIn               time.Duration
	awsSession                *session.Session
	logger                    *zap.Logger
}

func (outboxer *Outboxer) Outbox(requestId string) (err error) {

	logger := outboxer.logger
	defer logger.Sync()
	awsSession := outboxer.awsSession

	dynamodbClient := dynamodb.New(awsSession)
	sqsClient := sqs.New(awsSession)
	postman := notification.NewPostman(awsSession, outboxer.notificationTopicArn)

	defer func() {
		if err != nil {
			postman.SendNotification(requestId, err.Error())
		}
	}()

	logger = logger.With(zap.String("requestId", requestId))
	logger.Info("outboxing workflows ...")

	workflowsTable := workflows.WorkflowRecordTable{
		Table:          database.Table[workflows.WorkflowRecord]{Name: outboxer.workflowsTableName},
		DynamodbClient: dynamodbClient,
	}

	openWorkflowIndex := workflows.OpenWorkflowsIndex{
		TableName:      outboxer.workflowsTableName,
		IndexName:      outboxer.openWorkflowsIndexName,
		DynamodbClient: dynamodbClient,
	}

	now := time.Now()

	workflowRecords, err := openWorkflowIndex.OpenWorkflows(outboxer.amountOfWorkflowsToOutbox, zulu.DateTimeFromTime(now))

	logger = logger.With(zap.Int("amountOfWorkflows", len(workflowRecords)))
	logger.Info("fetched open workflows")

	if err != nil {
		logger.Error("impossible to fetch open workflows", zap.Error(err))
	}

	var errorsFromEventSending []error

	defer func() {
		if len(errorsFromEventSending) > 0 {
			postman.SendNotification(requestId, fmt.Sprintf("impossible to send %d events", len(errorsFromEventSending)))
		}
	}()

	for _, workflowRecord := range workflowRecords {
		logger = logger.With(zap.String("eventId", workflowRecord.EventId.String()))
		logger = logger.With(zap.String("targetQueueUrl", workflowRecord.TargetQueueUrl))
		logger.Info("sending event ...")
		_, errFromEventSending := sqsClient.SendMessage(&sqs.SendMessageInput{
			MessageBody:            aws.String(workflowRecord.Event),
			QueueUrl:               aws.String(workflowRecord.TargetQueueUrl),
			MessageGroupId:         aws.String(workflowRecord.EventMessageGroupId),
			MessageDeduplicationId: aws.String(workflowRecord.EventMessageDeduplicationId()),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"EventId": {
					DataType:    aws.String("String"),
					StringValue: aws.String(workflowRecord.EventId.String()),
				},
				"TargetQueueUrl": {
					DataType:    aws.String("String"),
					StringValue: aws.String(workflowRecord.TargetQueueUrl),
				},
			},
		})

		if errFromEventSending != nil {
			logger.Error("impossible to send event", zap.Error(errFromEventSending))
			errorsFromEventSending = append(errorsFromEventSending, errFromEventSending)
		}

		logger.Info("event sent. Postponing workflow ...")
		now := time.Now()
		nextStartAt := now.Add(outboxer.nextStartIn)
		err = workflowsTable.Postpone(workflowRecord, zulu.DateTimeFromTime(nextStartAt))

		if err != nil {
			logger.Error("impossible to postpone workflow", zap.Error(err))
			return
		}
		logger.Info("workflow postponed")

	}

	logger.Info("workflows outboxed")
	return
}
