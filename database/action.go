package database

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var ErrNotFound = fmt.Errorf("record not found")

func (table Table[R]) Action(dynamodbClient *dynamodb.DynamoDB) TableAction[R] {
	return TableAction[R]{
		Table:          table,
		DynamodbClient: dynamodbClient,
	}
}

type TableAction[R Record] struct {
	Table[R]
	DynamodbClient *dynamodb.DynamoDB
}

func (table TableAction[R]) Reconstitute(recordWithKey *R) (err error) {
	if recordWithKey == nil {
		return
	}
	primaryKey := (*recordWithKey).ThePrimaryKey()
	keys := map[string]*dynamodb.AttributeValue{
		primaryKey.PartitionKey.Name: primaryKey.PartitionKey.AttributeValue(),
	}
	if primaryKey.SortKey != nil {
		keys[primaryKey.SortKey.Name] = primaryKey.SortKey.AttributeValue()
	}
	result, err := table.DynamodbClient.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table.Table.Name),
		Key:       keys,
	})
	if err != nil {
		return
	}
	if len(result.Item) == 0 {
		return ErrNotFound
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, recordWithKey)
	if err != nil {
		return
	}
	return
}

func (table TableAction[R]) Persist(record R) (err error) {

	items, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return
	}

	_, err = table.DynamodbClient.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table.Table.Name),
		Item:      items,
	})
	return
}

func (table TableAction[R]) Query(partitionKey DynamodbKey, cursor *string, limit int) (records []R, nextCursor *string, err error) {

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(table.Name),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :the_partition_key", partitionKey.Name)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":the_partition_key": partitionKey.AttributeValue(),
		},
		ScanIndexForward: aws.Bool(false),
	}

	if cursor != nil {
		exclusiveStartKey, errOfDecoding := decodeCursor(*cursor)
		if errOfDecoding != nil {
			err = errOfDecoding
			return
		}
		queryInput.ExclusiveStartKey = exclusiveStartKey
	}

	queryInput.Limit = aws.Int64(int64(limit))

	items, err := table.DynamodbClient.Query(queryInput)
	if err != nil {
		return
	}

	records = make([]R, len(items.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(items.Items, &records)
	if err != nil {
		return
	}

	nextCursor, err = encodeCursor(items.LastEvaluatedKey)
	if err != nil {
		return
	}
	return
}

func decodeCursor(cursor string) (exclusiveStartKey map[string]*dynamodb.AttributeValue, err error) {
	var decodedCursor []byte
	decodedCursor, err = base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return
	}
	cursorAttributes := map[string]AttributeValueWrapper{}
	err = json.Unmarshal(decodedCursor, &cursorAttributes)
	if err != nil {
		return
	}

	exclusiveStartKey = map[string]*dynamodb.AttributeValue{}
	for key, value := range cursorAttributes {
		exclusiveStartKey[key] = value.AttributeValue
	}
	return
}

func encodeCursor(exclusiveStartKey map[string]*dynamodb.AttributeValue) (cursor *string, err error) {
	if len(exclusiveStartKey) == 0 {
		return
	}
	cursorAttributes := map[string]*AttributeValueWrapper{}
	for key, value := range exclusiveStartKey {
		cursorAttributes[key] = &AttributeValueWrapper{value}
	}
	var cursorBytes []byte
	cursorBytes, err = json.Marshal(cursorAttributes)
	if err != nil {
		return
	}
	cursorCandidate := base64.StdEncoding.EncodeToString(cursorBytes)
	cursor = &cursorCandidate
	return
}

type AttributeValueWrapper struct {
	*dynamodb.AttributeValue
}

func (avw *AttributeValueWrapper) MarshalJSON() ([]byte, error) {
	jsonAV := toJson(avw.AttributeValue)
	return json.Marshal(jsonAV)
}

func (avw *AttributeValueWrapper) UnmarshalJSON(data []byte) error {
	var jsonAV AttributeValueJSON
	if err := json.Unmarshal(data, &jsonAV); err != nil {
		return err
	}
	avw.AttributeValue = fromJson(&jsonAV)
	return nil
}

type AttributeValueJSON struct {
	B    []byte                         `json:"B,omitempty"`
	BOOL *bool                          `json:"BOOL,omitempty"`
	BS   [][]byte                       `json:"BS,omitempty"`
	L    []*AttributeValueJSON          `json:"L,omitempty"`
	M    map[string]*AttributeValueJSON `json:"M,omitempty"`
	N    *string                        `json:"N,omitempty"`
	NS   []*string                      `json:"NS,omitempty"`
	NULL *bool                          `json:"NULL,omitempty"`
	S    *string                        `json:"S,omitempty"`
	SS   []*string                      `json:"SS,omitempty"`
}

func toJson(av *dynamodb.AttributeValue) *AttributeValueJSON {
	if av == nil {
		return nil
	}

	jsonAV := &AttributeValueJSON{}

	if av.B != nil {
		jsonAV.B = av.B
	}
	if av.BOOL != nil {
		jsonAV.BOOL = av.BOOL
	}
	if av.BS != nil && len(av.BS) > 0 {
		jsonAV.BS = av.BS
	}
	if av.L != nil && len(av.L) > 0 {
		jsonAV.L = listToJson(av.L)
	}
	if av.M != nil && len(av.M) > 0 {
		jsonAV.M = mapToJson(av.M)
	}
	if av.N != nil {
		jsonAV.N = av.N
	}
	if av.NS != nil && len(av.NS) > 0 {
		jsonAV.NS = av.NS
	}
	if av.NULL != nil {
		jsonAV.NULL = av.NULL
	}
	if av.S != nil {
		jsonAV.S = av.S
	}
	if av.SS != nil && len(av.SS) > 0 {
		jsonAV.SS = av.SS
	}

	return jsonAV
}

func listToJson(list []*dynamodb.AttributeValue) []*AttributeValueJSON {
	if list == nil {
		return nil
	}
	result := make([]*AttributeValueJSON, len(list))
	for i, item := range list {
		result[i] = toJson(item)
	}
	return result
}

func mapToJson(m map[string]*dynamodb.AttributeValue) map[string]*AttributeValueJSON {
	if m == nil {
		return nil
	}
	result := make(map[string]*AttributeValueJSON)
	for key, value := range m {
		result[key] = toJson(value)
	}
	return result
}

func fromJson(jsonAV *AttributeValueJSON) *dynamodb.AttributeValue {
	if jsonAV == nil {
		return nil
	}

	return &dynamodb.AttributeValue{
		B:    jsonAV.B,
		BOOL: jsonAV.BOOL,
		BS:   jsonAV.BS,
		L:    listFromJSON(jsonAV.L),
		M:    mapFromJSON(jsonAV.M),
		N:    jsonAV.N,
		NS:   jsonAV.NS,
		NULL: jsonAV.NULL,
		S:    jsonAV.S,
		SS:   jsonAV.SS,
	}
}

func listFromJSON(list []*AttributeValueJSON) []*dynamodb.AttributeValue {
	if list == nil {
		return nil
	}
	result := make([]*dynamodb.AttributeValue, len(list))
	for i, item := range list {
		result[i] = fromJson(item)
	}
	return result
}

func mapFromJSON(m map[string]*AttributeValueJSON) map[string]*dynamodb.AttributeValue {
	if m == nil {
		return nil
	}
	result := make(map[string]*dynamodb.AttributeValue)
	for key, value := range m {
		result[key] = fromJson(value)
	}
	return result
}
