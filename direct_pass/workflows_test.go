package direct_pass

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/gitlotto/common/workflows"
	"github.com/gitlotto/common/zulu"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_open_workflowRecord_should_be_reconstituted_from_the_dynamodb_event(t *testing.T) {

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":null}`, partitionKey)
	eventGroupId := uuid.New().String()

	eventId := fmt.Sprintf("%s#%s", tableName, partitionKey)

	newImage := map[string]events.DynamoDBAttributeValue{
		"event_id":               events.NewStringAttribute(eventId),
		"created_at":             events.NewStringAttribute(createdAt.String()),
		"start_at":               events.NewStringAttribute(startAt.String()),
		"amount_of_starts":       events.NewNumberAttribute("0"),
		"target_queue_url":       events.NewStringAttribute(targetQueueUrl),
		"is_open":                events.NewStringAttribute(string(workflows.Open)),
		"event":                  events.NewStringAttribute(event),
		"event_message_group_id": events.NewStringAttribute(eventGroupId),
	}

	expectedWorkflow, err := workflows.NewFifoWorkflowRecord(tableName, partitionKey, nil, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)

	actualWorkflow, err := unmarshalWorkflow(newImage)
	assert.NoError(t, err)

	assert.Equal(t, *expectedWorkflow, actualWorkflow)
}

func Test_closed_workflowRecord_should_be_reconstituted_from_the_dynamodb_event(t *testing.T) {

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"
	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":null}`, partitionKey)
	eventGroupId := uuid.New().String()

	eventId := fmt.Sprintf("%s#%s", tableName, partitionKey)

	newImage := map[string]events.DynamoDBAttributeValue{
		"event_id":               events.NewStringAttribute(eventId),
		"created_at":             events.NewStringAttribute(createdAt.String()),
		"start_at":               events.NewStringAttribute(startAt.String()),
		"amount_of_starts":       events.NewNumberAttribute("0"),
		"target_queue_url":       events.NewStringAttribute(targetQueueUrl),
		"finished_at":            events.NewStringAttribute(finishedAt.String()),
		"event":                  events.NewStringAttribute(event),
		"event_message_group_id": events.NewStringAttribute(eventGroupId),
	}

	expectedWorkflow, err := workflows.NewFifoWorkflowRecord(tableName, partitionKey, nil, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	expectedWorkflow.FinishedAt = &finishedAt
	expectedWorkflow.IsOpen = nil
	assert.NoError(t, err)

	actualWorkflow, err := unmarshalWorkflow(newImage)
	assert.NoError(t, err)

	assert.Equal(t, *expectedWorkflow, actualWorkflow)
}
