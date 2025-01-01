package direct_pass

import (
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/notification"
	"github.com/gitlotto/common/workflows"
	"github.com/gitlotto/common/zulu"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DirectPasser struct {
	workflowsTableName   string
	notificationTopicArn string
	nextStartIn          time.Duration
	awsSession           *session.Session
	logger               *zap.Logger
}

func (passer *DirectPasser) Pass(event events.DynamoDBEvent) (err error) {

	logger := passer.logger
	defer logger.Sync()
	awsSession := passer.awsSession

	dynamodbClient := dynamodb.New(awsSession)
	sqsClient := sqs.New(awsSession)
	postman := notification.NewPostman(awsSession, passer.notificationTopicArn)

	requestId := uuid.New().String()

	defer func() {
		if err != nil {
			postman.SendNotification(requestId, err.Error())
		}
	}()

	logger = logger.With(zap.String("requestId", requestId))
	logger.Info("outboxing workflows ...")

	workflowsTable := workflows.WorkflowRecordTable{
		Table:          database.Table[workflows.WorkflowRecord]{Name: passer.workflowsTableName},
		DynamodbClient: dynamodbClient,
	}

	processSingle := func(record events.DynamoDBEventRecord, logger *zap.Logger) {
		var err error
		defer func() {
			if err != nil {
				postman.SendNotification(record.EventID, fmt.Sprintf("impossible to directly pass the event %s to SQS", record.EventID))
			}
		}()

		logger = logger.With(zap.String("dynamodbEventID", record.EventID))
		logger.Info("processing single record ...")

		if record.EventName != "INSERT" {
			logger.Info("skipping non-insert event")
			return
		}

		workflowRecord, err := unmarshalWorkflow(record.Change.NewImage)
		if err != nil {
			logger.Error("impossible to unmarshal workflow record", zap.Error(err))
			return
		}

		now := time.Now()

		if workflowRecord.StartAt.ToTime().After(now) {
			logger.Info("workflow is not ready to be passed")
			return
		}

		logger = logger.With(zap.String("eventId", workflowRecord.EventId))
		logger = logger.With(zap.String("targetQueueUrl", workflowRecord.TargetQueueUrl))
		logger.Info("sending event ...")
		_, err = sqsClient.SendMessage(&sqs.SendMessageInput{
			MessageBody:            aws.String(workflowRecord.Event),
			QueueUrl:               aws.String(workflowRecord.TargetQueueUrl),
			MessageGroupId:         aws.String(workflowRecord.EventMessageGroupId),
			MessageDeduplicationId: aws.String(workflowRecord.EventMessageDeduplicationId()),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"EventId": {
					DataType:    aws.String("String"),
					StringValue: aws.String(workflowRecord.EventId),
				},
				"TargetQueueUrl": {
					DataType:    aws.String("String"),
					StringValue: aws.String(workflowRecord.TargetQueueUrl),
				},
			},
		})

		if err != nil {
			logger.Error("impossible to send event", zap.Error(err))
			return
		}

		logger.Info("event sent. Postponing workflow ...")
		nextStartAt := now.Add(passer.nextStartIn)
		err = workflowsTable.Postpone(workflowRecord, zulu.DateTimeFromTime(nextStartAt))

		if err != nil {
			logger.Error("impossible to postpone workflow", zap.Error(err))
			return
		}
		logger.Info("workflow postponed")
	}

	for _, record := range event.Records {
		processSingle(record, logger)
	}

	logger.Info("workflows outboxed")
	return
}
