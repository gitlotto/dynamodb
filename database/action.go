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

func (table TableAction[R]) Query(partitionKey string, cursor *PrimaryKey, limit int) (records []R, nextCursor *PrimaryKey, err error) {

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(table.Name),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :the_partition_key", table.Schema.PartitionKeyName)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":the_partition_key": {
				S: aws.String(partitionKey),
			},
		},
		ScanIndexForward: aws.Bool(false),
	}

	if cursor != nil {
		queryInput.ExclusiveStartKey = map[string]*dynamodb.AttributeValue{
			cursor.PartitionKey.Name: {
				S: aws.String(cursor.PartitionKey.Value),
			},
			cursor.SortKey.Name: {
				S: aws.String(cursor.SortKey.Value),
			},
		}
	}

	queryInput.Limit = aws.Int64(int64(limit))

	items, err := table.DynamodbClient.Query(queryInput)
	if err != nil {
		return
	}

	records = make([]R, len(items.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(items.Items, &records)

	if len(items.LastEvaluatedKey) != 0 {
		var nextCursorCandidate PrimaryKey
		nextPartitionKey, nextPartitionKeyExists := items.LastEvaluatedKey[table.Schema.PartitionKeyName]
		if !nextPartitionKeyExists {
			return nil, nil, fmt.Errorf("missing partition key in last evaluated key")
		}
		nextCursorCandidate = PrimaryKey{
			PartitionKey: DynamodbKey{
				Name:  table.Schema.PartitionKeyName,
				Value: *nextPartitionKey.S,
			},
		}
		if table.Schema.SortKeyName != nil {
			nextSortKey, nextSortKeyExists := items.LastEvaluatedKey[*table.Schema.SortKeyName]
			if !nextSortKeyExists {
				return nil, nil, fmt.Errorf("missing sort key in last evaluated key")
			}
			nextCursorCandidate.SortKey = &DynamodbKey{
				Name:  *table.Schema.SortKeyName,
				Value: *nextSortKey.S,
			}
		}
		nextCursor = &nextCursorCandidate
	}
	return
}
