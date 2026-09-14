package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
)

// The job gate is a pure function of the broker's snapshot and the job's reading (ADR 13).
func TestJobGate(t *testing.T) {
	quiet := time.Minute
	reading := func(offsets map[string]map[int]int64, idle time.Duration) flink.Reading {
		return flink.Reading{Offsets: offsets, IdleFor: idle}
	}
	cases := []struct {
		name     string
		snapshot map[string]string
		reading  flink.Reading
		want     decide.JobGate
		note     string
	}{
		{"caught up and quiet", map[string]string{"t": "10,3"}, reading(map[string]map[int]int64{"t": {0: 9, 1: 2}}, time.Hour), decide.JobIdle, ""},
		{"records still to emit", map[string]string{"t": "401"}, reading(map[string]map[int]int64{"t": {0: 249}}, 0), decide.JobBusy, "151 records pending"},
		{"caught up but emitted recently", map[string]string{"t": "10"}, reading(map[string]map[int]int64{"t": {0: 9}}, 20*time.Second), decide.JobBusy, "emitted a record 20s ago"},
		{"never emitted, idle: nothing to read from where it started", map[string]string{"t": "500"}, reading(map[string]map[int]int64{"t": {0: flink.InitialOffset}}, time.Hour), decide.JobIdle, ""},
		{"never emitted, not idle: about to read", map[string]string{"t": "500"}, reading(map[string]map[int]int64{"t": {0: flink.InitialOffset}}, 0), decide.JobBusy, "500 records pending"},
		{"empty partition never emitted", map[string]string{"t": "0"}, reading(map[string]map[int]int64{"t": {0: flink.InitialOffset}}, time.Hour), decide.JobIdle, ""},
		{"job reads none of the declared topic", map[string]string{"t": "10"}, reading(map[string]map[int]int64{"u": {0: 9}}, time.Hour), decide.JobUnknown, "no offsets for t"},
		{"job lacks a partition", map[string]string{"t": "10,10"}, reading(map[string]map[int]int64{"t": {0: 9}}, time.Hour), decide.JobUnknown, "partition 1"},
		{"emitted past the snapshot is not negative", map[string]string{"t": "10"}, reading(map[string]map[int]int64{"t": {0: 12}}, time.Hour), decide.JobIdle, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, note := jobGate([]string{"t"}, c.snapshot, c.reading, quiet)
			if got != c.want || !strings.Contains(note, c.note) {
				t.Fatalf("gate=%v note=%q", got, note)
			}
		})
	}
}
