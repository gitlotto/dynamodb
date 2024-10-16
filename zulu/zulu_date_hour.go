package zulu

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DateHour struct {
	Date
	hour int
}

func (z DateHour) ToTime() time.Time {
	return time.Date(z.year, z.month, z.day, z.hour, 0, 0, 0, time.UTC)
}

func DateHourFromTime(t time.Time) DateHour {
	t = t.UTC()
	return DateHour{
		Date: DateFromTime(t),
		hour: t.Hour(),
	}
}

func DateHourFromString(s string) (DateHour, error) {
	t, err := time.Parse("2006-01-02T15Z", s)
	if err != nil {
		return DateHour{}, err
	}
	return DateHourFromTime(t), nil
}

func (z DateHour) String() string {
	return z.ToTime().Format("2006-01-02T15Z")
}

func (z DateHour) ToDate() Date {
	return z.Date
}

func (e DateHour) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(e.String())
	av.N = nil
	return nil
}

func (e *DateHour) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.S == nil || *av.S == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02T15Z", *av.S)
	t = t.UTC()
	if err != nil {
		return err
	}
	e.Date = DateFromTime(t)
	e.hour = t.Hour()
	return nil
}
