package database

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Record interface {
	Id() RecordIdentifier
}

type DynamodbKey struct {
	Name  string
	Value string
}

type PrimaryKey struct {
	PartitionKey DynamodbKey
	SortKey      *DynamodbKey
}

type RecordIdentifier struct {
	TableName  string
	PrimaryKey PrimaryKey
}

func Fetch[R Record](dynamodbClient *dynamodb.DynamoDB, emptyRecord R) (record *R, err error) {
	id := emptyRecord.Id()
	keys := map[string]*dynamodb.AttributeValue{
		id.PrimaryKey.PartitionKey.Name: {
			S: aws.String(id.PrimaryKey.PartitionKey.Value),
		},
	}
	if id.PrimaryKey.SortKey != nil {
		keys[id.PrimaryKey.SortKey.Name] = &dynamodb.AttributeValue{
			S: aws.String(id.PrimaryKey.SortKey.Value),
		}
	}
	result, err := dynamodbClient.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(id.TableName),
		Key:       keys,
	})
	if err != nil {
		return
	}
	if len(result.Item) == 0 {
		return
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &emptyRecord)
	if err != nil {
		return
	}
	record = &emptyRecord
	return
}

func Persist[R Record](dynamodbClient *dynamodb.DynamoDB, record R) (err error) {
	id := record.Id()

	items, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}

	_, err = dynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(id.TableName),
		Item:      items,
	})
	return
}
