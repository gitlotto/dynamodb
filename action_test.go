package database

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_SimpleRecord_should_be_stored_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := simpleRecord{
		PartitionKey: partitionKey,
		SomeValue:    "some value",
	}

	err = simpleRecordsTable.Action(dynamodbClient).Persist(record)
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
		"some_value": {
			S: aws.String("some value"),
		},
	}

	assert.Equal(t, expectedItems, actualItems)

	expectedRecord := record

	actualRecord := simpleRecord{}
	err = dynamodbattribute.UnmarshalMap(getEventOutput.Item, &actualRecord)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecord, actualRecord)

}

func Test_SimpleRecord_should_be_fetched_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := simpleRecord{
		PartitionKey: partitionKey,
		SomeValue:    "some value",
	}

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
		"some_value": {
			S: aws.String("some value"),
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

	recordWithOnlyKey := simpleRecord{
		PartitionKey: partitionKey,
	}

	actualRecord, err := simpleRecordsTable.Action(dynamodbClient).Fetch(recordWithOnlyKey)
	assert.NoError(t, err)

	expectedRecord := record
	assert.Equal(t, expectedRecord, *actualRecord)

}

func Test_Fetch_should_return_nil_if_the_simple_record_does_not_exist(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := simpleRecord{
		PartitionKey: partitionKey,
	}

	actualRecord, err := simpleRecordsTable.Action(dynamodbClient).Fetch(record)
	assert.NoError(t, err)
	assert.Nil(t, actualRecord)

}

func Test_SimpleRecord_should_be_reconstituted_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := simpleRecord{
		PartitionKey: partitionKey,
		SomeValue:    "some value",
	}

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
		"some_value": {
			S: aws.String("some value"),
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

	recordWithOnlyKey := simpleRecord{
		PartitionKey: partitionKey,
	}

	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&recordWithOnlyKey)
	assert.NoError(t, err)

	expectedRecord := record
	actualRecord := recordWithOnlyKey
	assert.Equal(t, expectedRecord, actualRecord)

}

func Test_Reconstitut_should_return_error_if_the_simple_record_does_not_exist(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	record := simpleRecord{
		PartitionKey: partitionKey,
	}

	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&record)
	assert.ErrorIs(t, err, ErrNotFound)

}

func Test_CompositeRecord_should_be_stored_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()

	record := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		SomeValue:    "some value",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(record)
	assert.NoError(t, err)

	getEventOutput, err := dynamodbClient.GetItem(
		&dynamodb.GetItemInput{
			TableName: aws.String(compositeRecordsTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"partition_key": {
					S: aws.String(partitionKey),
				},
				"sort_key": {
					S: aws.String(sortKey),
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
		"sort_key": {
			S: aws.String(sortKey),
		},
		"some_value": {
			S: aws.String("some value"),
		},
	}

	assert.Equal(t, expectedItems, actualItems)

	expectedRecord := record

	actualRecord := compositeRecord{}
	err = dynamodbattribute.UnmarshalMap(getEventOutput.Item, &actualRecord)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecord, actualRecord)

}

func Test_CompositeRecord_should_be_fetched_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()

	record := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		SomeValue:    "some value",
	}

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
		"sort_key": {
			S: aws.String(sortKey),
		},
		"some_value": {
			S: aws.String("some value"),
		},
	}

	actualItems, err := dynamodbattribute.MarshalMap(record)
	assert.NoError(t, err)
	assert.Equal(t, expectedItems, actualItems)

	_, err = dynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(compositeRecordsTableName),
		Item:      actualItems,
	})
	assert.NoError(t, err)

	recordWithOnlyKey := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}

	actualRecord, err := compositeRecordsTable.Action(dynamodbClient).Fetch(recordWithOnlyKey)
	assert.NotNil(t, actualRecord)
	assert.NoError(t, err)

	expectedRecord := record
	assert.Equal(t, expectedRecord, *actualRecord)

}

func Test_Fetch_should_return_nil_if_the_composite_record_does_not_exist(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()

	record := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}

	actualRecord, err := compositeRecordsTable.Action(dynamodbClient).Fetch(record)
	assert.NoError(t, err)
	assert.Nil(t, actualRecord)

}

func Test_CompositeRecord_should_be_reconstituted_in_correct_form(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()

	record := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		SomeValue:    "some value",
	}

	expectedItems := map[string]*dynamodb.AttributeValue{
		"partition_key": {
			S: aws.String(partitionKey),
		},
		"sort_key": {
			S: aws.String(sortKey),
		},
		"some_value": {
			S: aws.String("some value"),
		},
	}

	actualItems, err := dynamodbattribute.MarshalMap(record)
	assert.NoError(t, err)
	assert.Equal(t, expectedItems, actualItems)

	_, err = dynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(compositeRecordsTableName),
		Item:      actualItems,
	})
	assert.NoError(t, err)

	recordWithOnlyKey := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}

	err = compositeRecordsTable.Action(dynamodbClient).Reconstitute(&recordWithOnlyKey)
	assert.NoError(t, err)

	expectedRecord := record
	actualRecord := recordWithOnlyKey
	assert.Equal(t, expectedRecord, actualRecord)

}

func Test_Reconstitut_should_return_error_if_the_composite_record_does_not_exist(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()
	sortKey := uuid.New().String()

	record := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}

	err = compositeRecordsTable.Action(dynamodbClient).Reconstitute(&record)
	assert.ErrorIs(t, err, ErrNotFound)

}
