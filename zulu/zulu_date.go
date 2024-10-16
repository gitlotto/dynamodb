package zulu

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Date struct {
	year  int
	month time.Month
	day   int
}

func (z Date) ToTime() time.Time {
	return time.Date(z.year, z.month, z.day, 0, 0, 0, 0, time.UTC)
}

func DateFromTime(t time.Time) Date {
	t = t.UTC()
	return Date{year: t.Year(), month: t.Month(), day: t.Day()}
}

func DateFromString(s string) (Date, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Date{}, err
	}
	return DateFromTime(t), nil
}

func (z Date) String() string {
	return z.ToTime().Format("2006-01-02")
}

func (e Date) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(e.String())
	av.N = nil
	return nil
}

func (e *Date) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.S == nil || *av.S == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02", *av.S)
	if err != nil {
		return err
	}
	t = t.UTC()
	e.year = t.Year()
	e.month = t.Month()
	e.day = t.Day()
	return nil
}
