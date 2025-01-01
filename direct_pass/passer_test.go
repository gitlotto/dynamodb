package direct_pass

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/logging"
	"github.com/gitlotto/common/queue"
	"github.com/gitlotto/common/workflows"
	"github.com/gitlotto/common/zulu"
)

var awsConfig = aws.Config{
	Region:     aws.String("us-east-1"),
	Endpoint:   aws.String("http://localhost:4566"), // this is the LocalStack endpoint for all services
	DisableSSL: aws.Bool(true),
}

var awsSession = session.Must(session.NewSession(&awsConfig))
var logger = logging.MustCreateZuluTimeLogger()

const sevenHours time.Duration = time.Hour * 7

const workflowsTableName = "direct_passer_dynamodb-workflows"

var passer = DirectPasser{
	workflowsTableName:   workflowsTableName,
	notificationTopicArn: "arn:aws:sns:us-east-1:000000000000:direct_passer_notification-Notifications.fifo",
	nextStartIn:          sevenHours,
	awsSession:           awsSession,
	logger:               logger,
}

var dynamodbClient = dynamodb.New(awsSession)
var sqsClient = sqs.New(awsSession)

var notificationQueueUrl = "http://localhost:4566/000000000000/direct_passer_notification-Notifications.fifo"

var queueName = "http://localhost:4566/000000000000/direct_passer_queues.fifo"

var workflowsDynamodbTable = database.Table[workflows.WorkflowRecord]{
	Name: workflowsTableName,
}

func Test_Workflow_Direct_passer_should_write_the_workflow_into_the_sqs(t *testing.T) {
	var err error

	startOfTesting := time.Now()
	startOfTesting = startOfTesting.Add(-time.Second)

	workflowStartAt := time.Now().Add(-time.Hour * 1)
	workflow := makeFifoWorkflowRecord(queueName, workflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(workflow)
	assert.NoError(t, err)

	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   uuid.New().String(),
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"event_id":               events.NewStringAttribute(workflow.EventId),
						"target_queue_url":       events.NewStringAttribute(workflow.TargetQueueUrl),
						"start_at":               events.NewStringAttribute(workflow.StartAt.String()),
						"created_at":             events.NewStringAttribute(workflow.CreatedAt.String()),
						"amount_of_starts":       events.NewNumberAttribute(fmt.Sprintf("%d", workflow.AmountOfStarts)),
						"event":                  events.NewStringAttribute(workflow.Event),
						"event_message_group_id": events.NewStringAttribute(workflow.EventMessageGroupId),
						"is_open":                events.NewStringAttribute("OPEN"),
					},
				},
			},
		},
	}

	err = passer.Pass(event)
	assert.NoError(t, err)

	stratOfChecking := time.Now()
	stratOfChecking = stratOfChecking.Add(time.Second)

	lastNCommandsFromQueue, err := queue.GetLastNCommands(sqsClient, queueName, 1)
	assert.NoError(t, err)

	actualEventsFromQueue := make([]string, 1)
	for i, command := range lastNCommandsFromQueue {
		actualEventsFromQueue[i] = *command.Body
	}

	expectedEventsFromQueue := []string{workflow.Event}
	assert.ElementsMatch(t, expectedEventsFromQueue, actualEventsFromQueue)

	actualWorkflow := workflows.WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: queueName,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualWorkflow)
	assert.WithinRange(t, actualWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours), stratOfChecking.Add(sevenHours))
	expectedAmountOfStartOfWorkflow := workflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfWorkflow, actualWorkflow.AmountOfStarts)
}

func Test_Workflow_Direct_passer_should_not_write_the_workflow_into_the_sqs_if_the_event_is_not_a_creation_event(t *testing.T) {
	var err error

	workflowStartAt := time.Now().Add(-time.Hour * 1)
	workflow := makeFifoWorkflowRecord(queueName, workflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(workflow)
	assert.NoError(t, err)

	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   uuid.New().String(),
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"event_id":               events.NewStringAttribute(workflow.EventId),
						"target_queue_url":       events.NewStringAttribute(workflow.TargetQueueUrl),
						"start_at":               events.NewStringAttribute(workflow.StartAt.String()),
						"created_at":             events.NewStringAttribute(workflow.CreatedAt.String()),
						"amount_of_starts":       events.NewNumberAttribute(fmt.Sprintf("%d", workflow.AmountOfStarts)),
						"event":                  events.NewStringAttribute(workflow.Event),
						"event_message_group_id": events.NewStringAttribute(workflow.EventMessageGroupId),
						"is_open":                events.NewStringAttribute("OPEN"),
					},
				},
			},
		},
	}

	err = passer.Pass(event)
	assert.NoError(t, err)

	lastNCommandsFromQueue, err := queue.GetLastNCommands(sqsClient, queueName, 1)
	assert.NoError(t, err)

	assert.Empty(t, lastNCommandsFromQueue)

	actualWorkflow := workflows.WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: queueName,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.Equal(t, workflow, actualWorkflow)
}

func Test_Workflow_Direct_passer_should_not_write_the_workflow_into_the_sqs_if_the_start_date_of_the_event_Has_not_arrived_yet(t *testing.T) {
	var err error

	workflowStartAt := time.Now().Add(time.Hour * 1)
	workflow := makeFifoWorkflowRecord(queueName, workflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(workflow)
	assert.NoError(t, err)

	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   uuid.New().String(),
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"event_id":               events.NewStringAttribute(workflow.EventId),
						"target_queue_url":       events.NewStringAttribute(workflow.TargetQueueUrl),
						"start_at":               events.NewStringAttribute(workflow.StartAt.String()),
						"created_at":             events.NewStringAttribute(workflow.CreatedAt.String()),
						"amount_of_starts":       events.NewNumberAttribute(fmt.Sprintf("%d", workflow.AmountOfStarts)),
						"event":                  events.NewStringAttribute(workflow.Event),
						"event_message_group_id": events.NewStringAttribute(workflow.EventMessageGroupId),
						"is_open":                events.NewStringAttribute("OPEN"),
					},
				},
			},
		},
	}

	err = passer.Pass(event)
	assert.NoError(t, err)

	lastNCommandsFromQueue, err := queue.GetLastNCommands(sqsClient, queueName, 1)
	assert.NoError(t, err)

	assert.Empty(t, lastNCommandsFromQueue)

	actualWorkflow := workflows.WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: queueName,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.Equal(t, workflow, actualWorkflow)
}

func Test_Workflow_Outboxer_should_notify_if_it_fails_to_publish_an_event(t *testing.T) {
	var err error

	workflowStartAt := time.Now().Add(-time.Hour * 1)
	workflow := makeFifoWorkflowRecord(queueName, workflowStartAt)
	unknownQueue := "http://localhost:4566/000000000000/unknown_queue.fifo"
	workflow.TargetQueueUrl = unknownQueue
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(workflow)
	assert.NoError(t, err)

	eventId := uuid.New().String()

	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   eventId,
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"event_id":               events.NewStringAttribute(workflow.EventId),
						"target_queue_url":       events.NewStringAttribute(workflow.TargetQueueUrl),
						"start_at":               events.NewStringAttribute(workflow.StartAt.String()),
						"created_at":             events.NewStringAttribute(workflow.CreatedAt.String()),
						"event":                  events.NewStringAttribute(workflow.Event),
						"event_message_group_id": events.NewStringAttribute(workflow.EventMessageGroupId),
						"is_open":                events.NewStringAttribute("OPEN"),
					},
				},
			},
		},
	}

	err = passer.Pass(event)
	assert.NoError(t, err)

	lastNCommandsFromQueue, err := queue.GetLastNCommands(sqsClient, queueName, 1)
	assert.NoError(t, err)

	assert.Empty(t, lastNCommandsFromQueue)

	actualWorkflow := workflows.WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: unknownQueue,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.Equal(t, workflow, actualWorkflow)

	lastNCommandsFromNotificationQueue, err := queue.GetLastNCommands(sqsClient, notificationQueueUrl, 1)
	assert.NoError(t, err)

	expectedNotification := fmt.Sprintf(
		`{"requestId":"%s","message":"impossible to directly pass the event %s to SQS"}`,
		eventId,
		eventId,
	)
	actualNotification := lastNCommandsFromNotificationQueue[0].Body

	assert.JSONEq(t, expectedNotification, *actualNotification)
}

func makeFifoWorkflowRecord(targetQueueUrl string, startAt time.Time) workflows.WorkflowRecord {
	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.September, 16, 12, 45, 14, 0, time.UTC))
	event := uuid.New().String()
	eventGroupId := uuid.New().String()
	workflow, err := workflows.NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, zulu.DateTimeFromTime(startAt), targetQueueUrl, event, eventGroupId)
	if err != nil {
		panic(err)
	}
	return *workflow
}
