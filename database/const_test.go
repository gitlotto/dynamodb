package database

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var awsConfig = aws.Config{
	Region:     aws.String("us-east-1"),
	Endpoint:   aws.String("http://localhost:4566"), // this is the LocalStack endpoint for all services
	DisableSSL: aws.Bool(true),
}

var awsSession = session.Must(session.NewSession(&awsConfig))

var dynamodbClient = dynamodb.New(awsSession)

const simpleRecordsTableName = "database.simpleRecords"

type simpleRecord struct {
	PartitionKey string `dynamodbav:"partition_key"`
	SomeValue    string `dynamodbav:"some_value"`
}

func (record simpleRecord) ThePrimaryKey() PrimaryKey {
	return PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: record.PartitionKey,
			Type:  KeyTypeString,
		},
	}
}

var simpleRecordsTable = Table[simpleRecord]{
	Schema: SimpleSchema("partition_key"),
	Name:   simpleRecordsTableName,
}

const compositeRecordsTableName = "database.compositeRecords"

type compositeRecord struct {
	PartitionKey string `dynamodbav:"partition_key"`
	SortKey      int    `dynamodbav:"sort_key"`
	SomeValue    string `dynamodbav:"some_value"`
}

func (record compositeRecord) ThePrimaryKey() PrimaryKey {
	return PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: record.PartitionKey,
			Type:  KeyTypeString,
		},
		SortKey: &DynamodbKey{
			Name:  "sort_key",
			Value: strconv.Itoa(record.SortKey),
			Type:  KeyTypeNumber,
		},
	}
}

var compositeRecordsTable = Table[compositeRecord]{
	Name:   compositeRecordsTableName,
	Schema: CompositeSchema("partition_key", "sort_key"),
}
