package notification

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/google/uuid"
)

const onlyOneMessageGroupId string = "0"

type Postman struct {
	snsClient *sns.SNS
	topicArn  string
}

func NewPostman(session *session.Session, topicArn string) *Postman {
	return &Postman{
		snsClient: sns.New(session),
		topicArn:  topicArn,
	}
}

func (postman *Postman) SendNotification(requestId string, message string) (err error) {
	notification := JobNotification{
		RequestId: requestId,
		Message:   message,
	}

	messageBytes, err := json.Marshal(notification)
	if err != nil {
		return
	}

	messageBody := string(messageBytes)

	deduplicationId := uuid.New().String()

	_, err = postman.snsClient.Publish(&sns.PublishInput{
		Message:                aws.String(messageBody),
		TopicArn:               &postman.topicArn,
		MessageGroupId:         aws.String(onlyOneMessageGroupId),
		MessageDeduplicationId: aws.String(deduplicationId),
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"NotificationType": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Job"),
			},
		},
	})
	return
}
