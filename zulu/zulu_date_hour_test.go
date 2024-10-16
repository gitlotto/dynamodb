package zulu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ZuluDateHour_can_be_parsed_correctly(t *testing.T) {

	dateInString := "2020-06-01T12Z"
	actualZuluDateHour, err := DateHourFromString(dateInString)
	expectedZuluDateHour := DateHour{Date{2020, 6, 1}, 12}

	assert.NoError(t, err)
	assert.Equal(t, actualZuluDateHour, expectedZuluDateHour)
}

func Test_ZuluDateHour_can_be_converted_into_string_correctly(t *testing.T) {

	zuluDateHour := DateHour{Date{2020, 6, 1}, 12}
	actualString := zuluDateHour.String()
	expectedString := "2020-06-01T12Z"

	assert.Equal(t, expectedString, actualString)
}
