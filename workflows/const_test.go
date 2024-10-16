package workflows

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gitlotto/common/database"
)

const workflowsTableName = "workflows-workflows"
const openWorkflowsIndexName = "workflows-openWorkflows"

var awsConfig = aws.Config{
	Region:     aws.String("us-east-1"),
	Endpoint:   aws.String("http://localhost:4566"), // this is the LocalStack endpoint for all services
	DisableSSL: aws.Bool(true),
}

var awsSession = session.Must(session.NewSession(&awsConfig))
var dynamodbClient = dynamodb.New(awsSession)

type Event struct {
	PartitionKey string  `json:"partitionKey"`
	SortKey      *string `json:"sortKey,omitempty"`
}

var workflowRecordTable = WorkflowRecordTable{
	Table:          database.Table[WorkflowRecord]{Name: workflowsTableName},
	DynamodbClient: dynamodbClient,
}

var openWorkflowsIndex = OpenWorkflowsIndex{
	TableName:      workflowsTableName,
	IndexName:      openWorkflowsIndexName,
	DynamodbClient: dynamodbClient,
}

func deleteAllWorkflows() (err error) {
	workflows, err := dynamodbClient.Scan(&dynamodb.ScanInput{
		TableName: aws.String(workflowsTableName),
	})
	if err != nil {
		return
	}

	for _, item := range workflows.Items {
		_, err = dynamodbClient.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(workflowsTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"event_id":         item["event_id"],
				"target_queue_url": item["target_queue_url"],
			},
		})
		if err != nil {
			return
		}
	}

	return
}
