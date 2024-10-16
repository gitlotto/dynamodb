package zulu

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ZuluDate_can_be_parsed_correctly(t *testing.T) {

	dateInString := "2020-06-01"
	actualZuluDate, err := DateFromString(dateInString)
	expectedZuluDate := Date{
		year:  2020,
		month: time.June,
		day:   1,
	}

	assert.NoError(t, err)
	assert.Equal(t, actualZuluDate, expectedZuluDate)
}

func Test_ZuluDate_can_be_converted_into_string_correctly(t *testing.T) {

	zuluDate := Date{
		year:  2020,
		month: time.June,
		day:   1,
	}
	actualString := zuluDate.String()
	expectedString := "2020-06-01"

	assert.Equal(t, actualString, expectedString)
}
