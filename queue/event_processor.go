package queue

import (
	"github.com/aws/aws-lambda-go/events"
	"go.uber.org/zap"
)

type EventProcessor interface {
	ProcessSingle(event *events.SQSMessage, logger *zap.Logger) (err error)
}

func ProcessMultiple(
	sqsEvents events.SQSEvent,
	eventProcessor EventProcessor,
	logger *zap.Logger,
) (commandsProcessed events.SQSEventResponse) {

	logger.Info("Processing events in total", zap.Int("events", len(sqsEvents.Records)))

	failures := []events.SQSBatchItemFailure{}

	for _, event := range sqsEvents.Records {
		errOfTheMessage := eventProcessor.ProcessSingle(&event, logger)
		if errOfTheMessage != nil {
			eventFailure := &events.SQSBatchItemFailure{
				ItemIdentifier: event.MessageId,
			}
			failures = append(failures, *eventFailure)
		}
	}

	if len(failures) > 0 {
		logger.Error("Some events failed", zap.Int("eventFailures", len(failures)))
		commandsProcessed.BatchItemFailures = failures
	}
	return
}
