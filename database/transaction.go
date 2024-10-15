package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var ErrConditionalCheckFailed = fmt.Errorf("ConditionalCheckFailed")

type Transaction struct {
	transactionResults []*transactionResult
}

type transactionResult struct {
	transactionWriteItem *dynamodb.TransactWriteItem
	err                  error
}

func NewTransaction() *Transaction {
	return &Transaction{
		transactionResults: []*transactionResult{},
	}
}

func (transaction *Transaction) Include(writeItem *dynamodb.TransactWriteItem, err error) (tr *Transaction) {
	result := &transactionResult{
		transactionWriteItem: writeItem,
		err:                  err,
	}
	transaction.transactionResults = append(transaction.transactionResults, result)
	return transaction
}

func (transaction *Transaction) Execute(dynamodbClient *dynamodb.DynamoDB) (err error) {
	transactionWriteItems := []*dynamodb.TransactWriteItem{}
	for _, result := range transaction.transactionResults {
		if result.err != nil {
			err = result.err
			return
		}
		transactionWriteItems = append(transactionWriteItems, result.transactionWriteItem)
	}
	_, err = dynamodbClient.TransactWriteItems(
		&dynamodb.TransactWriteItemsInput{
			TransactItems: transactionWriteItems,
		},
	)

	switch errRefined := err.(type) {
	case *dynamodb.ConditionalCheckFailedException:
		err = ErrConditionalCheckFailed
		return
	case *dynamodb.TransactionCanceledException:
		for _, reason := range errRefined.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				err = ErrConditionalCheckFailed
				return
			}
		}
	case error:
		return
	}
	return
}
