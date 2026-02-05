package internal

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestHeaderCarrierGet(t *testing.T) {
	testCases := []struct {
		name     string
		carrier  HeaderCarrier
		key      string
		expected string
	}{
		{
			name: "exists",
			carrier: NewHeaderCarrier(&nats.Header{
				"foo": []string{"bar"},
			}),
			key:      "foo",
			expected: "bar",
		},
		{
			name:     "not exists",
			carrier:  NewHeaderCarrier(&nats.Header{}),
			key:      "foo",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.carrier.Get(tc.key)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestHeaderCarrierWithNilPointer(t *testing.T) {
	carrier := NewHeaderCarrier(nil)

	assert.Equal(t, "", carrier.Get("foo"))

	carrier.Set("foo", "bar")

	assert.Equal(t, []string{}, carrier.Keys())
}

func TestHeaderCarrierSet(t *testing.T) {
	var headers nats.Header
	carrier := NewHeaderCarrier(&headers)

	carrier.Set("foo", "bar")
	carrier.Set("foo", "bar2")
	carrier.Set("foo2", "bar3")

	assert.Equal(t, nats.Header{
		"foo":  []string{"bar2"},
		"foo2": []string{"bar3"},
	}, headers)
}

func TestHeaderCarrierKeys(t *testing.T) {
	testCases := []struct {
		name     string
		carrier  HeaderCarrier
		expected []string
	}{
		{
			name: "one",
			carrier: NewHeaderCarrier(&nats.Header{
				"foo": []string{"bar"},
			}),
			expected: []string{"foo"},
		},
		{
			name:     "none",
			carrier:  NewHeaderCarrier(&nats.Header{}),
			expected: []string{},
		},
		{
			name: "many",
			carrier: NewHeaderCarrier(&nats.Header{
				"foo": []string{"bar"},
				"baz": []string{"quux"},
			}),
			expected: []string{"foo", "baz"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.carrier.Keys()
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}
