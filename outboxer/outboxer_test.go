package outboxer

import (
	"fmt"
	"testing"
	"time"

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

const workflowsTableName = "outboxer_dynamodb-workflows"

var outboxer = Outboxer{
	workflowsTableName:        workflowsTableName,
	openWorkflowsIndexName:    "outboxer_dynamodb-openWorkflows",
	notificationTopicArn:      "arn:aws:sns:us-east-1:000000000000:outboxer_notification-Notifications.fifo",
	amountOfWorkflowsToOutbox: 3,
	nextStartIn:               sevenHours,
	awsSession:                awsSession,
	logger:                    logger,
}

var dynamodbClient = dynamodb.New(awsSession)
var sqsClient = sqs.New(awsSession)

var notificationQueueUrl = "http://localhost:4566/000000000000/outboxer_notification-Notifications.fifo"

var queueOne = "http://localhost:4566/000000000000/outboxer_random_queues-one.fifo"
var queueTwo = "http://localhost:4566/000000000000/outboxer_random_queues-two.fifo"

var workflowsDynamodbTable = database.Table[workflows.WorkflowRecord]{
	Name: workflowsTableName,
}

func Test_Workflow_Outboxer_should_pick_the_oldest_open_workflows_and_issue_events(t *testing.T) {
	var err error

	startOfTesting := time.Now()

	err = deleteAllWorkflows()
	assert.NoError(t, err)

	oldClosedWorkflowStartAt := startOfTesting.Add(-time.Hour * 5)
	oldClosedWorkflow := makeSimpleWorkflowRecord(queueOne, oldClosedWorkflowStartAt)
	oldClosedWorkflow.IsOpen = nil
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(oldClosedWorkflow)
	assert.NoError(t, err)

	firstOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 4)
	firstOpenWorkflow := makeSimpleWorkflowRecord(queueOne, firstOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(firstOpenWorkflow)
	assert.NoError(t, err)

	secondOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 3)
	secondOpenWorkflow := makeFifoWorkflowRecord(queueTwo, secondOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(secondOpenWorkflow)
	assert.NoError(t, err)

	thirdOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 2)
	thirdOpenWorkflow := makeSimpleWorkflowRecord(queueOne, thirdOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(thirdOpenWorkflow)
	assert.NoError(t, err)

	fourthOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 1)
	fourthOpenWorkflow := makeFifoWorkflowRecord(queueTwo, fourthOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(fourthOpenWorkflow)
	assert.NoError(t, err)

	requestId := uuid.New().String()
	err = outboxer.Outbox(requestId)
	assert.NoError(t, err)

	stratOfChecking := time.Now()

	lastNCommandsFromQueueOne, err := queue.GetLastNCommands(sqsClient, queueOne, 2)
	assert.NoError(t, err)

	actualEventsFromQueueOne := make([]string, 2)
	for i, command := range lastNCommandsFromQueueOne {
		actualEventsFromQueueOne[i] = *command.Body
	}

	expectedEventsFromQueueOne := []string{
		firstOpenWorkflow.Event, thirdOpenWorkflow.Event,
	}

	assert.ElementsMatch(t, expectedEventsFromQueueOne, actualEventsFromQueueOne)

	lastNCommandsFromQueueTwo, err := queue.GetLastNCommands(sqsClient, queueTwo, 1)
	assert.NoError(t, err)

	actualEventsFromQueueTwo := make([]string, 1)
	for i, command := range lastNCommandsFromQueueTwo {
		actualEventsFromQueueTwo[i] = *command.Body
	}

	expectedEventsFromQueueTwo := []string{
		secondOpenWorkflow.Event,
	}

	assert.ElementsMatch(t, expectedEventsFromQueueTwo, actualEventsFromQueueTwo)
	actualFirstWorkflow := workflows.WorkflowRecord{
		EventId:        firstOpenWorkflow.EventId,
		TargetQueueUrl: queueOne,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualFirstWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualFirstWorkflow)
	assert.WithinRange(t, actualFirstWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours-time.Second), stratOfChecking.Add(sevenHours+time.Second))
	expectedAmountOfStartOfFirstWorkflow := firstOpenWorkflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfFirstWorkflow, actualFirstWorkflow.AmountOfStarts)

	actualSecondWorkflow := workflows.WorkflowRecord{
		EventId:        secondOpenWorkflow.EventId,
		TargetQueueUrl: queueTwo,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualSecondWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualSecondWorkflow)
	assert.WithinRange(t, actualSecondWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours-time.Second), stratOfChecking.Add(sevenHours+time.Second))
	expectedAmountOfStartOfSecondWorkflow := secondOpenWorkflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfSecondWorkflow, actualSecondWorkflow.AmountOfStarts)

	actualThirdWorkflow := workflows.WorkflowRecord{
		EventId:        thirdOpenWorkflow.EventId,
		TargetQueueUrl: queueOne,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualThirdWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualThirdWorkflow)
	assert.WithinRange(t, actualThirdWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours-time.Second), stratOfChecking.Add(sevenHours+time.Second))
	expectedAmountOfStartOfThirdWorkflow := thirdOpenWorkflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfThirdWorkflow, actualThirdWorkflow.AmountOfStarts)

	actualFourthWorkflow := workflows.WorkflowRecord{
		EventId:        fourthOpenWorkflow.EventId,
		TargetQueueUrl: queueTwo,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualFourthWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualFourthWorkflow)
	assert.Equal(t, fourthOpenWorkflow, actualFourthWorkflow)

	actualOldClosedWorkflow := workflows.WorkflowRecord{
		EventId:        oldClosedWorkflow.EventId,
		TargetQueueUrl: queueOne,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualOldClosedWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualOldClosedWorkflow)
	assert.Equal(t, oldClosedWorkflow, actualOldClosedWorkflow)

}

func Test_Workflow_Outboxer_should_notify_if_it_fails_to_publish_an_event(t *testing.T) {
	var err error

	startOfTesting := time.Now()

	err = deleteAllWorkflows()
	assert.NoError(t, err)

	firstOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 4)
	firstOpenWorkflow := makeSimpleWorkflowRecord(queueOne, firstOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(firstOpenWorkflow)
	assert.NoError(t, err)

	secondOpenWorkflowStartAt := startOfTesting.Add(-time.Hour * 3)
	unknownQueue := "http://localhost:4566/000000000000/unknown_queue.fifo"
	secondOpenWorkflow := makeFifoWorkflowRecord(unknownQueue, secondOpenWorkflowStartAt)
	err = workflowsDynamodbTable.Action(dynamodbClient).Persist(secondOpenWorkflow)
	assert.NoError(t, err)

	requestId := uuid.New().String()
	err = outboxer.Outbox(requestId)
	assert.NoError(t, err)

	stratOfChecking := time.Now()

	lastNCommandsFromQueueOne, err := queue.GetLastNCommands(sqsClient, queueOne, 1)
	assert.NoError(t, err)

	actualEventsFromQueueOne := make([]string, 1)
	for i, command := range lastNCommandsFromQueueOne {
		actualEventsFromQueueOne[i] = *command.Body
	}

	expectedEventsFromQueueOne := []string{firstOpenWorkflow.Event}

	assert.ElementsMatch(t, expectedEventsFromQueueOne, actualEventsFromQueueOne)
	actualFirstWorkflow := workflows.WorkflowRecord{
		EventId:        firstOpenWorkflow.EventId,
		TargetQueueUrl: queueOne,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualFirstWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualFirstWorkflow)
	assert.WithinRange(t, actualFirstWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours-time.Second), stratOfChecking.Add(sevenHours+time.Second))
	expectedAmountOfStartOfFirstWorkflow := firstOpenWorkflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfFirstWorkflow, actualFirstWorkflow.AmountOfStarts)

	actualSecondWorkflow := workflows.WorkflowRecord{
		EventId:        secondOpenWorkflow.EventId,
		TargetQueueUrl: unknownQueue,
	}
	err = workflowsDynamodbTable.Action(dynamodbClient).Reconstitute(&actualSecondWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualSecondWorkflow)
	assert.WithinRange(t, actualSecondWorkflow.StartAt.ToTime(), startOfTesting.Add(sevenHours-time.Second), stratOfChecking.Add(sevenHours+time.Second))
	expectedAmountOfStartOfSecondWorkflow := secondOpenWorkflow.AmountOfStarts + 1
	assert.Equal(t, expectedAmountOfStartOfSecondWorkflow, actualSecondWorkflow.AmountOfStarts)

	lastNCommandsFromNotificationQueue, err := queue.GetLastNCommands(sqsClient, notificationQueueUrl, 1)
	assert.NoError(t, err)

	expectedNotification := fmt.Sprintf(
		`{"requestId":"%s","message":"impossible to send 1 events"}`,
		requestId,
	)
	actualNotification := lastNCommandsFromNotificationQueue[0].Body

	assert.Equal(t, expectedNotification, *actualNotification)
}

func makeSimpleWorkflowRecord(targetQueueUrl string, startAt time.Time) workflows.WorkflowRecord {
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

func deleteAllWorkflows() (err error) {
	workflows, err := dynamodbClient.Scan(&dynamodb.ScanInput{
		TableName: aws.String(outboxer.workflowsTableName),
	})
	if err != nil {
		return
	}

	for _, item := range workflows.Items {
		_, err = dynamodbClient.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(outboxer.workflowsTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"event_id":         item["event_id"],
				"target_queue_url": item["target_queue_url"],
			},
		})
		if err != nil {
			return
		}
	}

	return
}
