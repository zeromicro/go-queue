package kq

import (
	"testing"
	"time"
)

func TestResolvePushTimeout(t *testing.T) {
	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{
			name: "uses configured timeout",
			got:  resolvePushTimeout(5 * time.Second),
			want: 5 * time.Second,
		},
		{
			name: "uses default for zero timeout",
			got:  resolvePushTimeout(0),
			want: defaultPushTimeout,
		},
		{
			name: "uses default for negative timeout",
			got:  resolvePushTimeout(-time.Second),
			want: defaultPushTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("resolvePushTimeout() = %v, want %v", tt.got, tt.want)
			}
		})
	}
}
