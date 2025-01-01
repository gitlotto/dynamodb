package workflows

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gitlotto/common/database"
	"github.com/gitlotto/common/zulu"
)

type WorkflowRecordTable struct {
	database.Table[WorkflowRecord]
	DynamodbClient *dynamodb.DynamoDB
}

var ErrWorkflowHadBeenFinished = errors.New("workflow had been finished")

func (table WorkflowRecordTable) Postpone(workflow WorkflowRecord, nextStartAt zulu.DateTime) (err error) {
	_, err = table.DynamodbClient.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(table.Table.Name),
		Key: map[string]*dynamodb.AttributeValue{
			"event_id": {
				S: aws.String(workflow.EventId),
			},
			"target_queue_url": {
				S: aws.String(workflow.TargetQueueUrl),
			},
		},
		UpdateExpression: aws.String("SET start_at = :start_at ADD amount_of_starts :by_one"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":start_at": {
				S: aws.String(nextStartAt.String()),
			},
			":by_one": {
				N: aws.String("1"),
			},
		},
		ConditionExpression: aws.String("attribute_exists(is_open)"),
	})
	switch errRefined := err.(type) {
	case *dynamodb.ConditionalCheckFailedException:
		return ErrWorkflowHadBeenFinished
	case *dynamodb.TransactionCanceledException:
		for _, reason := range errRefined.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				return ErrWorkflowHadBeenFinished
			}
		}
	default:
		return
	}
	return err
}

func (table WorkflowRecordTable) TransactionalClose(
	worflowEventId string,
	workflowTargetQueueUrl string,
	finishedAt zulu.DateTime,
) (item *dynamodb.TransactWriteItem, err error) {
	return &dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			TableName: aws.String(table.Table.Name),
			Key: map[string]*dynamodb.AttributeValue{
				"event_id": {
					S: aws.String(worflowEventId),
				},
				"target_queue_url": {
					S: aws.String(workflowTargetQueueUrl),
				},
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":finished_at": {
					S: aws.String(finishedAt.String()),
				},
			},
			UpdateExpression:    aws.String("SET finished_at = :finished_at REMOVE is_open"),
			ConditionExpression: aws.String("attribute_exists(is_open)"),
		},
	}, nil
}

func (table WorkflowRecordTable) Close(worflowEventId string, workflowTargetQueueUrl string, finishedAt zulu.DateTime) (err error) {
	_, err = table.DynamodbClient.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(table.Table.Name),
		Key: map[string]*dynamodb.AttributeValue{
			"event_id": {
				S: aws.String(worflowEventId),
			},
			"target_queue_url": {
				S: aws.String(workflowTargetQueueUrl),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":finished_at": {
				S: aws.String(finishedAt.String()),
			},
		},
		UpdateExpression:    aws.String("SET finished_at = :finished_at REMOVE is_open"),
		ConditionExpression: aws.String("attribute_exists(is_open)"),
	})
	switch errRefined := err.(type) {
	case *dynamodb.ConditionalCheckFailedException:
		return ErrWorkflowHadBeenFinished
	case *dynamodb.TransactionCanceledException:
		for _, reason := range errRefined.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				return ErrWorkflowHadBeenFinished
			}
		}
	default:
		return
	}
	return err
}
