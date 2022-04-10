package lruutil

import (
	"testing"
	"time"
)

func TestLruCache(t *testing.T) {
	tests := []struct {
		vals     []string
		interval time.Duration
		ttl      time.Duration
		expect   int
	}{
		{
			vals:     []string{"a", "b", "c", "d"},
			interval: time.Millisecond * 50,
			ttl:      time.Second,
			expect:   4,
		},
		{
			vals:     []string{"a", "b", "c", "d"},
			interval: time.Second * 2,
			ttl:      time.Second,
			expect:   0,
		},
	}
	for i, tt := range tests {
		sf := NewTimeEvictLru(tt.ttl)
		for _, v := range tt.vals {
			sf.Set(v, []byte(v))
		}
		time.Sleep(tt.interval)
		for _, v := range tt.vals {
			sf.Get(v)
		}
		if tt.expect != sf.Len() {
			t.Fatalf("#%d: expected %+v, got %+v", i, tt.expect, sf.Len())
		}
	}
}
