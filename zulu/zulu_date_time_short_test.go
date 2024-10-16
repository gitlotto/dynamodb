package zulu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ZuluDateTime_can_be_parsed_correctly(t *testing.T) {

	dateInString := "2020-06-01T12:34:56Z"
	actualZuluDateTime, err := DateTimeFromString(dateInString)
	expectedZuluDateTime := DateTime{Date{2020, 6, 1}, 12, 34, 56}

	assert.NoError(t, err)
	assert.Equal(t, actualZuluDateTime, expectedZuluDateTime)
}

func Test_ZuluDateTime_can_be_converted_into_string_correctly(t *testing.T) {

	zuluDateTime := DateTime{Date{2020, 6, 1}, 12, 34, 56}
	actualString := zuluDateTime.String()
	expectedString := "2020-06-01T12:34:56Z"

	assert.Equal(t, actualString, expectedString)
}
