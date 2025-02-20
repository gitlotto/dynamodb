package database

type Record interface {
	ThePrimaryKey() PrimaryKey
}

type PrimaryKey struct {
	PartitionKey DynamodbKey
	SortKey      *DynamodbKey
}

type Table[R Record] struct {
	Name string
}
