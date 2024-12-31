package direct_pass

import (
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/gitlotto/common/workflows"
	"github.com/gitlotto/common/zulu"
)

func ErrInvalidWorkflowRecord(field string) error {
	return errors.New("invalid workflow record: " + field)
}

func unmarshalWorkflow(attributes map[string]events.DynamoDBAttributeValue) (record workflows.WorkflowRecord, err error) {
	if attr, ok := attributes["event_id"]; ok && attr.DataType() == events.DataTypeString {
		record.EventId = attr.String()
	} else {
		err = ErrInvalidWorkflowRecord("event_id")
		return
	}

	if attr, ok := attributes["target_queue_url"]; ok && attr.DataType() == events.DataTypeString {
		record.TargetQueueUrl = attr.String()
	} else {
		err = ErrInvalidWorkflowRecord("target_queue_url")
		return
	}

	if attr, ok := attributes["created_at"]; ok && attr.DataType() == events.DataTypeString {
		record.CreatedAt, err = zulu.DateTimeFromString(attr.String())
		if err != nil {
			return
		}
	} else {
		err = ErrInvalidWorkflowRecord("created_at")
		return
	}

	if attr, ok := attributes["start_at"]; ok && attr.DataType() == events.DataTypeString {
		record.StartAt, err = zulu.DateTimeFromString(attr.String())
		if err != nil {
			return
		}
	} else {
		err = ErrInvalidWorkflowRecord("start_at")
		return
	}

	if attr, ok := attributes["amount_of_starts"]; ok && attr.DataType() == events.DataTypeNumber {
		var amountOfStarts int64
		amountOfStarts, err = attr.Integer()
		if err != nil {
			return
		}
		record.AmountOfStarts = int(amountOfStarts)
	} else {
		err = ErrInvalidWorkflowRecord("amount_of_starts")
	}

	if attr, ok := attributes["is_open"]; ok && attr.DataType() == events.DataTypeString {
		isOpen := workflows.Open
		record.IsOpen = &isOpen
	}

	if attr, ok := attributes["finished_at"]; ok && attr.DataType() == events.DataTypeString {
		var finishedAt zulu.DateTime
		finishedAt, err = zulu.DateTimeFromString(attr.String())
		if err != nil {
			return
		}
		record.FinishedAt = &finishedAt
	}

	if attr, ok := attributes["event"]; ok && attr.DataType() == events.DataTypeString {
		record.Event = attr.String()
	} else {
		err = ErrInvalidWorkflowRecord("event")
	}

	if attr, ok := attributes["event_message_group_id"]; ok && attr.DataType() == events.DataTypeString {
		record.EventMessageGroupId = attr.String()
	} else {
		err = ErrInvalidWorkflowRecord("event_message_group_id")
	}

	return
}
