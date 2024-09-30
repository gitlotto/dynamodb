package database

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const simpleRecordsTableName = "gitlotto.simpleRecords"

type SimpleRecord struct {
	PartitionKey string `dynamodbav:"partition_key"`
}

func (record SimpleRecord) Id() RecordIdentifier {
	return RecordIdentifier{
		TableName: simpleRecordsTableName,
		PrimaryKey: PrimaryKey{
			PartitionKey: DynamodbKey{
				Name:  "partition_key",
				Value: record.PartitionKey,
			},
		},
	}
}

func Test_SimpleRecord_should_be_stored_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := SimpleRecord{
		PartitionKey: partitionKey,
	}

	err = Persist(dynamodbClient, record)
	assert.NoError(t, err)

	getEventOutput, err := dynamodbClient.GetItem(
		&dynamodb.GetItemInput{
			TableName: aws.String(simpleRecordsTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"partition_key": {
					S: aws.String(partitionKey),
				},
			},
		},
	)

	assert.NoError(t, err)
	actualItems := getEventOutput.Item

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
	}

	assert.Equal(t, expectedItems, actualItems)

	expectedRecord := record

	actualRecord := SimpleRecord{}
	err = dynamodbattribute.UnmarshalMap(getEventOutput.Item, &actualRecord)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecord, actualRecord)

}

func Test_SimpleRecord_should_be_fetch_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := SimpleRecord{
		PartitionKey: partitionKey,
	}

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
	}

	actualItems, err := dynamodbattribute.MarshalMap(record)
	assert.NoError(t, err)
	assert.Equal(t, expectedItems, actualItems)

	_, err = dynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(simpleRecordsTableName),
		Item:      actualItems,
	})
	assert.NoError(t, err)

	actualRecord, err := Fetch(dynamodbClient, record)
	assert.NoError(t, err)

	expectedRecord := record
	assert.Equal(t, expectedRecord, *actualRecord)

}

func Test_Fetch_should_return_nil_if_the_simple_record_does_not_exist(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := SimpleRecord{
		PartitionKey: partitionKey,
	}

	actualRecord, err := Fetch(dynamodbClient, record)
	assert.NoError(t, err)
	assert.Nil(t, actualRecord)

}
