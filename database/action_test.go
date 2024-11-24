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

func Test_Reconstitute_should_return_error_if_the_composite_record_does_not_exist(t *testing.T) {
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

func Test_Querying_should_fetch_composite_records_from_the_beginning_if_no_cursor_is_provided(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 2
	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).Query(partitionKey, nil, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, fifthRecord)
	assert.Contains(t, actualRecords, fourthRecord)

	expectedCursor := PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: partitionKey,
		},
		SortKey: &DynamodbKey{
			Name:  "sort_key",
			Value: "4",
		},
	}
	assert.NotNil(t, nextCursor)
	assert.Equal(t, expectedCursor, *nextCursor)

}

func Test_Querying_should_fetch_composite_records_from_the_given_cursor(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 2
	startingCursor := PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: partitionKey,
		},
		SortKey: &DynamodbKey{
			Name:  "sort_key",
			Value: "4",
		},
	}

	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).Query(partitionKey, &startingCursor, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, thirdRecord)
	assert.Contains(t, actualRecords, secondRecord)

	expectedCursor := PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: partitionKey,
		},
		SortKey: &DynamodbKey{
			Name:  "sort_key",
			Value: "2",
		},
	}
	assert.NotNil(t, nextCursor)
	assert.Equal(t, expectedCursor, *nextCursor)

}

func Test_Querying_should_fetch_the_last_composite_records_and_return_nil_as_a_cursor(t *testing.T) {
	var err error

	partitionKey := uuid.New().String()

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 10
	startingCursor := PrimaryKey{
		PartitionKey: DynamodbKey{
			Name:  "partition_key",
			Value: partitionKey,
		},
		SortKey: &DynamodbKey{
			Name:  "sort_key",
			Value: "4",
		},
	}

	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).Query(partitionKey, &startingCursor, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, thirdRecord)
	assert.Contains(t, actualRecords, secondRecord)
	assert.Contains(t, actualRecords, firstRecord)

	assert.Nil(t, nextCursor)

}

func Test_QueryingV2_should_fetch_composite_records_from_the_beginning_if_no_cursor_is_provided(t *testing.T) {
	var err error

	partitionKey := "2231bbd9-8247-4115-941a-cf1dd87a6f1a"

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 2
	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).QueryV2(partitionKey, nil, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, fifthRecord)
	assert.Contains(t, actualRecords, fourthRecord)

	expectedCursor := "eyJwYXJ0aXRpb25fa2V5IjoiMjIzMWJiZDktODI0Ny00MTE1LTk0MWEtY2YxZGQ4N2E2ZjFhIiwic29ydF9rZXkiOiI0In0="
	assert.NotNil(t, nextCursor)
	assert.Equal(t, expectedCursor, *nextCursor)

}

func Test_QueryingV2_should_fetch_composite_records_from_the_given_cursor(t *testing.T) {
	var err error

	partitionKey := "bc8f6d9b-cc47-46e7-a18b-489b63d8dfc4"

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 2
	startingCursor := "eyJwYXJ0aXRpb25fa2V5IjoiYmM4ZjZkOWItY2M0Ny00NmU3LWExOGItNDg5YjYzZDhkZmM0IiwgInNvcnRfa2V5IjogIjQifQ=="

	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).QueryV2(partitionKey, &startingCursor, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, thirdRecord)
	assert.Contains(t, actualRecords, secondRecord)

	expectedCursor := "eyJwYXJ0aXRpb25fa2V5IjoiYmM4ZjZkOWItY2M0Ny00NmU3LWExOGItNDg5YjYzZDhkZmM0Iiwic29ydF9rZXkiOiIyIn0="
	assert.NotNil(t, nextCursor)
	assert.Equal(t, expectedCursor, *nextCursor)

}

func Test_QueryingV2_should_fetch_the_last_composite_records_and_return_nil_as_a_cursor(t *testing.T) {
	var err error

	partitionKey := "80f8f38f-8f63-4340-850d-fcbd6b95d826"

	firstRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "1",
		SomeValue:    "some value 1",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(firstRecord)
	assert.NoError(t, err)

	secondRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "2",
		SomeValue:    "some value 2",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(secondRecord)
	assert.NoError(t, err)

	thirdRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "3",
		SomeValue:    "some value 3",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(thirdRecord)
	assert.NoError(t, err)

	fourthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "4",
		SomeValue:    "some value 4",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fourthRecord)
	assert.NoError(t, err)

	fifthRecord := compositeRecord{
		PartitionKey: partitionKey,
		SortKey:      "5",
		SomeValue:    "some value 5",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(fifthRecord)
	assert.NoError(t, err)

	limit := 10
	startingCursor := "eyJwYXJ0aXRpb25fa2V5IjoiODBmOGYzOGYtOGY2My00MzQwLTg1MGQtZmNiZDZiOTVkODI2IiwgInNvcnRfa2V5IjogIjQifQ=="

	actualRecords, nextCursor, err := compositeRecordsTable.Action(dynamodbClient).QueryV2(partitionKey, &startingCursor, limit)
	assert.NoError(t, err)
	assert.Contains(t, actualRecords, thirdRecord)
	assert.Contains(t, actualRecords, secondRecord)
	assert.Contains(t, actualRecords, firstRecord)

	assert.Nil(t, nextCursor)

}
