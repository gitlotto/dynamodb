package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type arbitraryStruct struct {
	When time.Time `json:"when"`
}

func Test_Time_is_marshalled_in_zulu_format_when_created_via_constructor(t *testing.T) {

	now := time.Date(2023, 10, 25, 19, 57, 56, 123000000, time.UTC)
	arbitrary := arbitraryStruct{When: now}

	arbitraryMarshalled, err := json.Marshal(arbitrary)
	if assert.NoError(t, err) {
		assert.JSONEq(t, `{"when":"2023-10-25T19:57:56.123Z"}`, string(arbitraryMarshalled))
	}

}

func Test_Time_is_marshalled_in_zulu_format_when_created_from_string(t *testing.T) {

	now, err := time.Parse("2006-01-02T15:04:05.000Z", "2023-10-25T19:57:56.123Z")
	assert.NoError(t, err)

	arbitrary := arbitraryStruct{When: now}

	arbitraryMarshalled, err := json.Marshal(arbitrary)
	if assert.NoError(t, err) {
		assert.JSONEq(t, `{"when":"2023-10-25T19:57:56.123Z"}`, string(arbitraryMarshalled))
	}

}

// func Test_Time_is_marshalled_in_zulu_format_and_has_milis_when_created_from_string(t *testing.T) {

// 	now, err := time.Parse("2006-01-02T15:04:05.000Z", "2023-10-25T19:57:56.000Z")
// 	assert.NoError(t, err)

// 	arbitrary := arbitraryStruct{When: now}

// 	arbitraryMarshalled, err := json.Marshal(arbitrary)
// 	if assert.NoError(t, err) {
// 		assert.JSONEq(t, `{"when":"2023-10-25T19:57:56.000Z"}`, string(arbitraryMarshalled))
// 	}

// }
