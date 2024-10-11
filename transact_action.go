package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (table Table[R]) TransactInsert(
	record R,
) (item *dynamodb.TransactWriteItem, err error) {
	items, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}

	primaryKey := record.ThePrimaryKey()
	condition := fmt.Sprintf("attribute_not_exists(%s)", primaryKey.PartitionKey.Name)
	if primaryKey.SortKey != nil {
		condition = fmt.Sprintf("%s AND attribute_not_exists(%s)", condition, primaryKey.SortKey.Name)
	}

	item = &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName:           aws.String(table.Name),
			Item:                items,
			ConditionExpression: aws.String(condition),
		},
	}

	return
}
