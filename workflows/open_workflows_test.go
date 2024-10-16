package workflows

import (
	"testing"
	"time"

	"github.com/gitlotto/common/zulu"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_OpenWorkflowsIndex_should_read_oldest_open_workflows_from_the_table(t *testing.T) {
	var err error

	err = deleteAllWorkflows()
	assert.NoError(t, err)

	closedOldestWorkflowStartedAt := time.Date(2023, time.September, 16, 12, 45, 14, 0, time.UTC)
	closedOldestWorkflowRecord := makeWorkflowRecord(closedOldestWorkflowStartedAt)
	closedOldestWorkflowRecord.IsOpen = nil
	err = workflowRecordTable.Action(dynamodbClient).Persist(closedOldestWorkflowRecord)
	assert.NoError(t, err)

	openOldestWorkflowStartedAt := time.Date(2023, time.September, 17, 12, 45, 14, 0, time.UTC)
	openOldestWorkflowRecord := makeWorkflowRecord(openOldestWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openOldestWorkflowRecord)
	assert.NoError(t, err)

	openOlderWorkflowStartedAt := time.Date(2023, time.September, 18, 12, 45, 14, 0, time.UTC)
	openOlderWorkflowRecord := makeWorkflowRecord(openOlderWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openOlderWorkflowRecord)
	assert.NoError(t, err)

	openNewestWorkflowStartedAt := time.Date(2023, time.September, 19, 12, 45, 14, 0, time.UTC)
	openNewestWorkflowRecord := makeWorkflowRecord(openNewestWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openNewestWorkflowRecord)
	assert.NoError(t, err)

	takeUntil := zulu.DateTimeFromTime(time.Date(2023, time.September, 20, 12, 45, 14, 0, time.UTC))
	actualOldestOpenWorkflows, err := openWorkflowsIndex.OpenWorkflows(2, takeUntil)
	assert.NoError(t, err)

	expectedOldestOpenWorkflows := []WorkflowRecord{openOldestWorkflowRecord, openOlderWorkflowRecord}
	assert.ElementsMatch(t, expectedOldestOpenWorkflows, actualOldestOpenWorkflows)
}

func Test_OpenWorkflowsIndex_should_not_go_further_than_the_given_time(t *testing.T) {
	var err error

	err = deleteAllWorkflows()
	assert.NoError(t, err)

	closedOldestWorkflowStartedAt := time.Date(2023, time.September, 16, 12, 45, 14, 0, time.UTC)
	closedOldestWorkflowRecord := makeWorkflowRecord(closedOldestWorkflowStartedAt)
	closedOldestWorkflowRecord.IsOpen = nil
	err = workflowRecordTable.Action(dynamodbClient).Persist(closedOldestWorkflowRecord)
	assert.NoError(t, err)

	openOldestWorkflowStartedAt := time.Date(2023, time.September, 17, 12, 45, 14, 0, time.UTC)
	openOldestWorkflowRecord := makeWorkflowRecord(openOldestWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openOldestWorkflowRecord)
	assert.NoError(t, err)

	openOlderWorkflowStartedAt := time.Date(2023, time.September, 18, 12, 45, 14, 0, time.UTC)
	openOlderWorkflowRecord := makeWorkflowRecord(openOlderWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openOlderWorkflowRecord)
	assert.NoError(t, err)

	openNewestWorkflowStartedAt := time.Date(2023, time.September, 19, 12, 45, 14, 0, time.UTC)
	openNewestWorkflowRecord := makeWorkflowRecord(openNewestWorkflowStartedAt)
	err = workflowRecordTable.Action(dynamodbClient).Persist(openNewestWorkflowRecord)
	assert.NoError(t, err)

	takeUntil := zulu.DateTimeFromTime(time.Date(2023, time.September, 18, 11, 45, 14, 0, time.UTC))
	actualOldestOpenWorkflows, err := openWorkflowsIndex.OpenWorkflows(4, takeUntil)
	assert.NoError(t, err)

	expectedOldestOpenWorkflows := []WorkflowRecord{openOldestWorkflowRecord}
	assert.ElementsMatch(t, expectedOldestOpenWorkflows, actualOldestOpenWorkflows)
}

func makeWorkflowRecord(startAt time.Time) WorkflowRecord {
	tableName := uuid.New().String()
	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()
	createdAt := zulu.DateTimeFromTime(time.Date(2023, time.September, 16, 12, 45, 14, 0, time.UTC))
	targetQueueUrl := uuid.New().String() + ".fifo"
	event := "event"
	eventGroupId := uuid.New().String()

	workflow, err := NewFifoWorkflowRecord(tableName, partitionKey, &sortKey, createdAt, zulu.DateTimeFromTime(startAt), targetQueueUrl, event, eventGroupId)
	if err != nil {
		panic(err)
	}
	return *workflow
}
