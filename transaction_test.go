package database

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type simpleRecordTransactions struct {
	table Table[simpleRecord]
}

func (table simpleRecordTransactions) write(r simpleRecord) (transaction *dynamodb.TransactWriteItem, err error) {
	item, err := dynamodbattribute.MarshalMap(r)
	candidate := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      item,
			TableName: aws.String(table.table.Name),
		},
	}
	transaction = candidate
	return
}

func (table simpleRecordTransactions) writeIfDoesNotExist(r simpleRecord) (transaction *dynamodb.TransactWriteItem, err error) {
	item, err := dynamodbattribute.MarshalMap(r)
	candidate := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:                item,
			TableName:           aws.String(table.table.Name),
			ConditionExpression: aws.String("attribute_not_exists(partition_key)"),
		},
	}
	transaction = candidate
	return
}

type compositeReordsTransactions struct {
	table Table[compositeRecord]
}

func (table compositeReordsTransactions) write(record compositeRecord) (transaction *dynamodb.TransactWriteItem, err error) {
	item, err := dynamodbattribute.MarshalMap(record)
	candidate := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      item,
			TableName: aws.String(table.table.Name),
		},
	}
	transaction = candidate
	return
}

var simpleRecordTransactionsImpl = simpleRecordTransactions{
	table: simpleRecordsTable,
}

var compositeReordTransactionsImpl = compositeReordsTransactions{
	table: compositeRecordsTable,
}

func Test_Transaction_should_execute(t *testing.T) {
	var err error

	simpleRecord1 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 1",
	}

	simpleRecord2 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 2",
	}

	compositeRecord1 := compositeRecord{
		PartitionKey: uuid.New().String(),
		SortKey:      uuid.New().String(),
		SomeValue:    "some value",
	}

	err = NewTransaction().
		Include(simpleRecordTransactionsImpl.write(simpleRecord1)).
		Include(simpleRecordTransactionsImpl.write(simpleRecord2)).
		Include(compositeReordTransactionsImpl.write(compositeRecord1)).
		Execute(dynamodbClient)
	assert.NoError(t, err)

	actualSimpleRecord1 := simpleRecord{PartitionKey: simpleRecord1.PartitionKey}
	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&actualSimpleRecord1)
	assert.NoError(t, err)
	assert.Equal(t, simpleRecord1, actualSimpleRecord1)

	actualSimpleRecord2 := simpleRecord{PartitionKey: simpleRecord2.PartitionKey}
	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&actualSimpleRecord2)
	assert.NoError(t, err)
	assert.Equal(t, simpleRecord2, actualSimpleRecord2)

	actualCompositeRecord1 := compositeRecord{
		PartitionKey: compositeRecord1.PartitionKey,
		SortKey:      compositeRecord1.SortKey,
	}
	err = compositeRecordsTable.Action(dynamodbClient).Reconstitute(&actualCompositeRecord1)
	assert.NoError(t, err)
	assert.Equal(t, compositeRecord1, actualCompositeRecord1)
}

func Test_Transaction_should_handle_the_error_upon_the_failure_of_the_condition(t *testing.T) {
	var err error

	simpleRecord1 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 1",
	}

	simpleRecord2 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 2",
	}

	compositeRecord1 := compositeRecord{
		PartitionKey: uuid.New().String(),
		SortKey:      uuid.New().String(),
		SomeValue:    "some value",
	}

	err = simpleRecordsTable.Action(dynamodbClient).Persist(simpleRecord1)
	assert.NoError(t, err)

	err = NewTransaction().
		Include(simpleRecordTransactionsImpl.writeIfDoesNotExist(simpleRecord1)).
		Include(simpleRecordTransactionsImpl.write(simpleRecord2)).
		Include(compositeReordTransactionsImpl.write(compositeRecord1)).
		Execute(dynamodbClient)
	assert.ErrorIs(t, err, ErrConditionalCheckFailed)

	actualSimpleRecord2 := simpleRecord{PartitionKey: simpleRecord2.PartitionKey}
	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&actualSimpleRecord2)
	assert.ErrorIs(t, err, ErrNotFound)

	actualCompositeRecord1 := compositeRecord{
		PartitionKey: compositeRecord1.PartitionKey,
		SortKey:      compositeRecord1.SortKey,
	}
	err = compositeRecordsTable.Action(dynamodbClient).Reconstitute(&actualCompositeRecord1)
	assert.ErrorIs(t, err, ErrNotFound)
}
