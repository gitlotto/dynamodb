package database

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Record interface {
	ThePrimaryKey() PrimaryKey
}

type DynamodbKey struct {
	Name  string
	Value string
}

type PrimaryKey struct {
	PartitionKey DynamodbKey
	SortKey      *DynamodbKey
}

type DynamodbTable[R Record] struct {
	TableName      string
	DynamodbClient *dynamodb.DynamoDB
}

func (table DynamodbTable[R]) Fetch(emptyRecord R) (record *R, err error) {
	primaryKey := emptyRecord.ThePrimaryKey()
	keys := map[string]*dynamodb.AttributeValue{
		primaryKey.PartitionKey.Name: {
			S: aws.String(primaryKey.PartitionKey.Value),
		},
	}
	if primaryKey.SortKey != nil {
		keys[primaryKey.SortKey.Name] = &dynamodb.AttributeValue{
			S: aws.String(primaryKey.SortKey.Value),
		}
	}
	result, err := table.DynamodbClient.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
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

func (table DynamodbTable[R]) Persist(record R) (err error) {

	items, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}

	_, err = table.DynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table.TableName),
		Item:      items,
	})
	return
}
