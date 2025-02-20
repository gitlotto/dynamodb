package database

import "github.com/aws/aws-sdk-go/service/dynamodb"

type DynamodbKey struct {
	Name  string
	Value string
	Type  KeyType
}

type KeyType string

const (
	KeyTypeString KeyType = "S"
	KeyTypeNumber KeyType = "N"
)

func (key DynamodbKey) AttributeValue() (attr *dynamodb.AttributeValue) {
	switch key.Type {
	case KeyTypeNumber:
		attr = &dynamodb.AttributeValue{
			N: &key.Value,
		}
	case KeyTypeString:
		attr = &dynamodb.AttributeValue{
			S: &key.Value,
		}
	}
	return
}
