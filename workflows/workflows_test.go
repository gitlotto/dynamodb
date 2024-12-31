package workflows

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gitlotto/common/zulu"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_new_fifo_workflowRecord_should_not_be_created_if_queue_is_simple(t *testing.T) {

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String()

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":null}`, partitionKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, nil, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.Error(t, err)
	assert.Nil(t, workflow)
	assert.Equal(t, ErrFifoWorkflowQueueMismatch(targetQueueUrl), err)
}

func Test_new_fifo_workflowRecord_should_be_stored_in_correct_form(t *testing.T) {

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":null}`, partitionKey)
	eventGroupId := uuid.New().String()

	eventId := fmt.Sprintf("%s#%s", tableName, partitionKey)

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, nil, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	actualItems, err := dynamodbattribute.MarshalMap(*workflow)
	assert.NoError(t, err)
	expectedItems := map[string]*dynamodb.AttributeValue{
		"event_id": {
			S: aws.String(eventId),
		},
		"created_at": {
			S: aws.String("2023-10-15T12:45:14Z"),
		},
		"start_at": {
			S: aws.String("2023-10-16T12:45:14Z"),
		},
		"amount_of_starts": {
			N: aws.String("0"),
		},
		"target_queue_url": {
			S: aws.String(targetQueueUrl),
		},
		"is_open": {
			S: aws.String(string(Open)),
		},
		"event": {
			S: aws.String(event),
		},
		"event_message_group_id": {
			S: aws.String(eventGroupId),
		},
	}

	assert.Equal(t, expectedItems, actualItems)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        eventId,
		TargetQueueUrl: targetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)

	assert.NoError(t, err)
	assert.NotNil(t, actualWorkflow)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_Closed_WorkflowRecord_should_be_stored_in_correct(t *testing.T) {

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"
	eventGroupId := uuid.New().String()

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	workflow.IsOpen = nil
	amountOfStarts := 1
	workflow.AmountOfStarts = amountOfStarts
	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))
	workflow.FinishedAt = &finishedAt

	eventId := fmt.Sprintf("%s#%s#%s", tableName, partitionKey, sortKey)

	actualItems, err := dynamodbattribute.MarshalMap(*workflow)
	assert.NoError(t, err)
	expectedItems := map[string]*dynamodb.AttributeValue{
		"event_id": {
			S: aws.String(eventId),
		},
		"created_at": {
			S: aws.String("2023-10-15T12:45:14Z"),
		},
		"start_at": {
			S: aws.String("2023-10-16T12:45:14Z"),
		},
		"amount_of_starts": {
			N: aws.String("1"),
		},
		"target_queue_url": {
			S: aws.String(targetQueueUrl),
		},
		"finished_at": {
			S: aws.String("2023-10-17T12:45:14Z"),
		},
		"event": {
			S: aws.String(event),
		},
		"event_message_group_id": {
			S: aws.String(eventGroupId),
		},
	}

	assert.Equal(t, expectedItems, actualItems)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        eventId,
		TargetQueueUrl: targetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualWorkflow)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}
