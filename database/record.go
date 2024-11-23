package database

type Record interface {
	ThePrimaryKey() PrimaryKey
}

type PrimaryKey struct {
	PartitionKey DynamodbKey
	SortKey      *DynamodbKey
}

type Schema struct {
	PartitionKeyName string
	SortKeyName      *string
}

func SimpleSchema(partitionKeyName string) Schema {
	return Schema{
		PartitionKeyName: partitionKeyName,
	}
}

func CompositeSchema(partitionKeyName string, sortKeyName string) Schema {
	return Schema{
		PartitionKeyName: partitionKeyName,
		SortKeyName:      &sortKeyName,
	}
}

type Table[R Record] struct {
	Schema
	Name string
}
