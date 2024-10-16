package zulu

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DateTimeMillis struct {
	DateTime
	milli int
}

func (z DateTimeMillis) ToTime() time.Time {
	return time.Date(z.year, z.month, z.day, z.hour, z.minute, z.second, z.milli*1000000, time.UTC)
}

func DateTimeMillisFromTime(t time.Time) DateTimeMillis {
	t = t.UTC()
	return DateTimeMillis{
		DateTime: DateTimeFromTime(t),
		milli:    t.Nanosecond() / 1000000,
	}
}

func DateTimeMillisFromString(s string) (DateTimeMillis, error) {
	t, err := time.Parse("2006-01-02T15:04:05.000Z", s)
	t = t.UTC()
	if err != nil {
		return DateTimeMillis{}, err
	}
	return DateTimeMillisFromTime(t), nil
}

func (z DateTimeMillis) String() string {
	return z.ToTime().Format("2006-01-02T15:04:05.000Z")
}

func (z DateTimeMillis) ToDate() Date {
	return z.Date
}

func (e DateTimeMillis) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(e.String())
	av.N = nil
	return nil
}

func (e *DateTimeMillis) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.S == nil || *av.S == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02T15:04:05.000Z", *av.S)
	t = t.UTC()
	if err != nil {
		return err
	}
	e.DateTime = DateTimeFromTime(t)
	e.milli = t.Nanosecond() / 1000000
	return nil
}
