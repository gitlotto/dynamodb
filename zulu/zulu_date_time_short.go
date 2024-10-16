package zulu

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DateTime struct {
	Date
	hour   int
	minute int
	second int
}

func (z DateTime) ToTime() time.Time {
	return time.Date(z.year, z.month, z.day, z.hour, z.minute, z.second, 0, time.UTC)
}

func DateTimeFromTime(t time.Time) DateTime {
	t = t.UTC()
	return DateTime{
		Date:   DateFromTime(t),
		hour:   t.Hour(),
		minute: t.Minute(),
		second: t.Second(),
	}
}

func DateTimeFromString(s string) (DateTime, error) {
	t, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		return DateTime{}, err
	}
	return DateTimeFromTime(t), nil
}

func (z DateTime) String() string {
	return z.ToTime().Format("2006-01-02T15:04:05Z")
}

func (z DateTime) ToDate() Date {
	return z.Date
}

func (e DateTime) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(e.String())
	av.N = nil
	return nil
}

func (e *DateTime) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.S == nil || *av.S == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02T15:04:05Z", *av.S)
	t = t.UTC()
	if err != nil {
		return err
	}
	e.Date = DateFromTime(t)
	e.hour = t.Hour()
	e.minute = t.Minute()
	e.second = t.Second()
	return nil
}
