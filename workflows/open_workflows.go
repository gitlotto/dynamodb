package workflows

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gitlotto/common/zulu"
)

type OpenWorkflowsIndex struct {
	TableName      string
	IndexName      string
	DynamodbClient *dynamodb.DynamoDB
}

func (index OpenWorkflowsIndex) OpenWorkflows(limit int, until zulu.DateTime) (workflowRecords []WorkflowRecord, err error) {
	queryInput := &dynamodb.QueryInput{
		TableName:              &index.TableName,
		IndexName:              &index.IndexName,
		KeyConditionExpression: aws.String("is_open = :is_open AND start_at <= :start_at"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":is_open": {
				S: aws.String(string(Open)),
			},
			":start_at": {
				S: aws.String(until.String()),
			},
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int64(int64(limit)),
	}

	items, err := index.DynamodbClient.Query(queryInput)
	if err != nil {
		return
	}

	err = dynamodbattribute.UnmarshalListOfMaps(items.Items, &workflowRecords)

	return
}
