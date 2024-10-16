package zulu

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ZuluDateTimeMillis_can_be_parsed_correctly(t *testing.T) {

	dateInString := "2020-06-01T12:34:56.123Z"
	actualZuluDateTime, err := DateTimeMillisFromString(dateInString)
	expectedZuluDateTime := DateTimeMillis{
		DateTimeFromTime(time.Date(2020, time.June, 1, 12, 34, 56, 123000000, time.UTC)),
		123,
	}

	assert.NoError(t, err)
	assert.Equal(t, actualZuluDateTime, expectedZuluDateTime)
}

func Test_ZuluDateTimeMillis_can_be_converted_into_string_correctly(t *testing.T) {

	zuluDateTime := DateTimeMillis{
		DateTimeFromTime(time.Date(2020, time.June, 1, 12, 34, 56, 123000000, time.UTC)),
		0,
	}
	actualString := zuluDateTime.String()
	expectedString := "2020-06-01T12:34:56.000Z"

	assert.Equal(t, actualString, expectedString)
}
