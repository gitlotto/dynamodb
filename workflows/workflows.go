package workflows

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/zulu"
)

func ErrFifoWorkflowQueueMismatch(queueUrl string) error {
	return fmt.Errorf("fifo workflow should not delegate to a simple SQS queue %s", queueUrl)
}

type WorkflowRecord struct {
	EventId             string         `dynamodbav:"event_id"`
	TargetQueueUrl      string         `dynamodbav:"target_queue_url"`
	CreatedAt           zulu.DateTime  `dynamodbav:"created_at"`
	StartAt             zulu.DateTime  `dynamodbav:"start_at"`
	AmountOfStarts      int            `dynamodbav:"amount_of_starts"`
	IsOpen              *IsOpen        `dynamodbav:"is_open,omitempty"`
	FinishedAt          *zulu.DateTime `dynamodbav:"finished_at,omitempty"`
	Event               string         `dynamodbav:"event"`
	EventMessageGroupId string         `dynamodbav:"event_message_group_id"`
}

func (record WorkflowRecord) ThePrimaryKey() database.PrimaryKey {
	return database.PrimaryKey{
		PartitionKey: database.DynamodbKey{
			Name:  "event_id",
			Value: record.EventId,
		},
		SortKey: &database.DynamodbKey{
			Name:  "target_queue_url",
			Value: record.TargetQueueUrl,
		},
	}
}

func (record WorkflowRecord) EventMessageDeduplicationId() string {
	deduplicationIdInBytes := sha256.Sum256([]byte(record.EventId))
	deduplicationIdInString := hex.EncodeToString(deduplicationIdInBytes[:])
	return deduplicationIdInString
}

func NewFifoWorkflowRecord(
	tableName string,
	partitionKey string,
	sortKey *string,
	createdAt zulu.DateTime,
	startAt zulu.DateTime,
	targetQueueUrl string,
	event string,
	eventGroupId string,
) (*WorkflowRecord, error) {
	if targetQueueUrl == "" || !strings.HasSuffix(targetQueueUrl, ".fifo") {
		return nil, ErrFifoWorkflowQueueMismatch(targetQueueUrl)
	}
	isOpen := Open
	eventId := NewEventId(tableName, partitionKey, sortKey)
	amountOfStarts := 0
	workflowRecord := WorkflowRecord{
		EventId:             eventId.String(),
		CreatedAt:           createdAt,
		StartAt:             startAt,
		AmountOfStarts:      amountOfStarts,
		IsOpen:              &isOpen,
		FinishedAt:          nil,
		TargetQueueUrl:      targetQueueUrl,
		Event:               event,
		EventMessageGroupId: eventGroupId,
	}
	return &workflowRecord, nil
}

type IsOpen string

const (
	Open IsOpen = "OPEN"
)

type EventId struct {
	value string
}

func NewEventId(tableName string, partitionKey string, sortKey *string) EventId {
	idPrefix := tableName + "#" + partitionKey
	if sortKey == nil {
		return EventId{value: idPrefix}
	}
	return EventId{value: idPrefix + "#" + *sortKey}
}

func (id EventId) String() string {
	return id.value
}

func (id EventId) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = &id.value
	return nil
}

func (id *EventId) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.S == nil || *av.S == "" {
		return nil
	}

	id.value = *av.S
	return nil
}
