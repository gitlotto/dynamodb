package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var ErrNotFound = fmt.Errorf("record not found")

func (table Table[R]) Action(dynamodbClient *dynamodb.DynamoDB) TableAction[R] {
	return TableAction[R]{
		Table:          table,
		DynamodbClient: dynamodbClient,
	}
}

type TableAction[R Record] struct {
	Table[R]
	DynamodbClient *dynamodb.DynamoDB
}

func (table TableAction[R]) Reconstitute(recordWithKey *R) (err error) {
	if recordWithKey == nil {
		return
	}
	primaryKey := (*recordWithKey).ThePrimaryKey()
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
		TableName: aws.String(table.Table.Name),
		Key:       keys,
	})
	if err != nil {
		return
	}
	if len(result.Item) == 0 {
		return ErrNotFound
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, recordWithKey)
	if err != nil {
		return
	}
	return
}

func (table TableAction[R]) Persist(record R) (err error) {

	items, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}

	_, err = table.DynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table.Table.Name),
		Item:      items,
	})
	return
}
