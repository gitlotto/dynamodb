package database

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func Test_Transaction_insert_should_put_a_simple_record_into_the_database(t *testing.T) {
	var err error

	simpleRecord1 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 1",
	}

	err = NewTransaction().
		Include(simpleRecordsTable.TransactInsert(simpleRecord1)).
		Execute(dynamodbClient)
	assert.NoError(t, err)

	actualSimpleRecord1 := simpleRecord{PartitionKey: simpleRecord1.PartitionKey}
	err = simpleRecordsTable.Action(dynamodbClient).Reconstitute(&actualSimpleRecord1)
	assert.NoError(t, err)
	assert.Equal(t, simpleRecord1, actualSimpleRecord1)

}

func Test_Transaction_insert_should_put_a_composite_record_into_the_database(t *testing.T) {
	var err error

	compositeRecord1 := compositeRecord{
		PartitionKey: uuid.New().String(),
		SortKey:      rand.Int(),
		SomeValue:    "some value",
	}

	err = NewTransaction().
		Include(compositeRecordsTable.TransactInsert(compositeRecord1)).
		Execute(dynamodbClient)
	assert.NoError(t, err)

	actualCompositeRecord1 := compositeRecord{
		PartitionKey: compositeRecord1.PartitionKey,
		SortKey:      compositeRecord1.SortKey,
	}
	err = compositeRecordsTable.Action(dynamodbClient).Reconstitute(&actualCompositeRecord1)
	assert.NoError(t, err)
	assert.Equal(t, compositeRecord1, actualCompositeRecord1)
}

func Test_Transaction_insert_should_not_put_a_simple_record_into_the_database_if_it_has_been_saved_before(t *testing.T) {
	var err error

	simpleRecord1 := simpleRecord{
		PartitionKey: uuid.New().String(),
		SomeValue:    "some value 1",
	}

	err = simpleRecordsTable.Action(dynamodbClient).Persist(simpleRecord1)
	assert.NoError(t, err)

	err = NewTransaction().
		Include(simpleRecordsTable.TransactInsert(simpleRecord1)).
		Execute(dynamodbClient)
	assert.ErrorIs(t, err, ErrConditionalCheckFailed)
}

func Test_Transaction_insert_should_not_put_a_composite_record_into_the_database_if_it_has_been_saved_before(t *testing.T) {
	var err error

	compositeRecord1 := compositeRecord{
		PartitionKey: uuid.New().String(),
		SortKey:      rand.Int(),
		SomeValue:    "some value",
	}

	err = compositeRecordsTable.Action(dynamodbClient).Persist(compositeRecord1)
	assert.NoError(t, err)

	err = NewTransaction().
		Include(compositeRecordsTable.TransactInsert(compositeRecord1)).
		Execute(dynamodbClient)
	assert.ErrorIs(t, err, ErrConditionalCheckFailed)

}
