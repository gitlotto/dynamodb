package workflows

import (
	"fmt"
	"testing"
	"time"

	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/zulu"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_WorkflowRecordTable_should_postpone_the_workflow_if_it_is_still_open(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	nextStartAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = workflowRecordTable.Postpone(*workflow, nextStartAt)
	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}

	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)

	expectedWorkflow := *workflow
	expectedWorkflow.StartAt = nextStartAt
	expectedWorkflow.AmountOfStarts = 1

	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_not_postpone_the_workflow_if_it_had_been_closed(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	workflow.IsOpen = nil

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	nextStartAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = workflowRecordTable.Postpone(*workflow, nextStartAt)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrWorkflowHadBeenFinished)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_close_the_workflow_if_it_is_still_open(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = workflowRecordTable.Close(workflow.EventId.String(), workflow.TargetQueueUrl, finishedAt)
	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualWorkflow)

	expectedWorkflow := *workflow
	expectedWorkflow.FinishedAt = &finishedAt
	expectedWorkflow.IsOpen = nil

	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_not_close_the_workflow_if_it_had_been_closed(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	workflow.IsOpen = nil

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = workflowRecordTable.Close(workflow.EventId.String(), workflow.TargetQueueUrl, finishedAt)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrWorkflowHadBeenFinished)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_close_the_workflow_in_a_transaction_if_it_is_still_open(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = database.
		NewTransaction().
		Include(workflowRecordTable.TransactionalClose(workflow.EventId.String(), workflow.TargetQueueUrl, finishedAt)).
		Execute(dynamodbClient)

	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)
	assert.NotNil(t, actualWorkflow)

	expectedWorkflow := *workflow
	expectedWorkflow.FinishedAt = &finishedAt
	expectedWorkflow.IsOpen = nil

	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_not_close_the_workflow_in_a_transaction_if_it_had_been_closed(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	workflow.IsOpen = nil

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	finishedAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 17, 12, 45, 14, 0, time.UTC))

	err = database.
		NewTransaction().
		Include(workflowRecordTable.TransactionalClose(workflow.EventId.String(), workflow.TargetQueueUrl, finishedAt)).
		Execute(dynamodbClient)
	assert.Error(t, err)
	assert.ErrorIs(t, err, database.ErrConditionalCheckFailed)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_create_a_workflow_in_a_transaction_if_it_has_not_been_created(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	err = database.
		NewTransaction().
		Include(workflowRecordTable.TransactInsert(*workflow)).
		Execute(dynamodbClient)

	assert.NoError(t, err)

	actualWorkflow := WorkflowRecord{
		EventId:        workflow.EventId,
		TargetQueueUrl: workflow.TargetQueueUrl,
	}
	err = workflowRecordTable.Action(dynamodbClient).Reconstitute(&actualWorkflow)
	assert.NoError(t, err)

	expectedWorkflow := *workflow
	assert.Equal(t, expectedWorkflow, actualWorkflow)
}

func Test_WorkflowRecordTable_should_not_create_a_workflow_if_it_has_not_been_created(t *testing.T) {
	var err error

	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 15, 12, 45, 14, 0, time.UTC))
	startAt := zulu.DateTimeFromTime(time.Date(2023, time.October, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"

	event := fmt.Sprintf(`{"partitionKey":"%s","sortKey":"%s"}`, partitionKey, sortKey)
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, startAt, targetQueueUrl, event, eventGroupId)
	assert.NoError(t, err)
	assert.NotNil(t, workflow)

	err = workflowRecordTable.Action(dynamodbClient).Persist(*workflow)
	assert.NoError(t, err)

	err = database.
		NewTransaction().
		Include(workflowRecordTable.TransactInsert(*workflow)).
		Execute(dynamodbClient)

	assert.Error(t, err)
	assert.ErrorIs(t, err, database.ErrConditionalCheckFailed)
}
