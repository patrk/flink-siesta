package decide

import (
	"strings"
	"testing"
	"time"
)

// The job gate is a pure function of the broker's end offsets and the job's reading (ADR 13).
func TestJobGate(t *testing.T) {
	quiet := time.Minute
	reading := func(offsets map[string]map[int]int64, idle time.Duration) JobReading {
		return JobReading{Offsets: offsets, IdleFor: idle}
	}
	ends := func(v ...int64) map[string][]int64 { return map[string][]int64{"t": v} }
	cases := []struct {
		name    string
		ends    map[string][]int64
		reading JobReading
		want    JobGate
		note    string
	}{
		{"caught up and quiet", ends(10, 3), reading(map[string]map[int]int64{"t": {0: 9, 1: 2}}, time.Hour), JobIdle, ""},
		{"records still to emit", ends(401), reading(map[string]map[int]int64{"t": {0: 249}}, 0), JobBusy, "151 records pending"},
		{"caught up but emitted recently", ends(10), reading(map[string]map[int]int64{"t": {0: 9}}, 20*time.Second), JobBusy, "emitted a record 20s ago"},
		{"never emitted, idle: nothing to read from where it started", ends(500), reading(map[string]map[int]int64{"t": {0: InitialOffset}}, time.Hour), JobIdle, ""},
		{"never emitted, not idle: about to read", ends(500), reading(map[string]map[int]int64{"t": {0: InitialOffset}}, 0), JobBusy, "500 records pending"},
		{"empty partition never emitted", ends(0), reading(map[string]map[int]int64{"t": {0: InitialOffset}}, time.Hour), JobIdle, ""},
		{"job reads none of the declared topic", ends(10), reading(map[string]map[int]int64{"u": {0: 9}}, time.Hour), JobUnknown, "no offsets for t"},
		{"job lacks a partition", ends(10, 10), reading(map[string]map[int]int64{"t": {0: 9}}, time.Hour), JobUnknown, "partition 1"},
		{"broker lacks a partition", ends(10, -1), reading(map[string]map[int]int64{"t": {0: 9, 1: 9}}, time.Hour), JobUnknown, "no end offset"},
		{"emitted past the snapshot is not negative", ends(10), reading(map[string]map[int]int64{"t": {0: 12}}, time.Hour), JobIdle, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, note := jobGate([]string{"t"}, c.ends, c.reading, quiet)
			if got != c.want || !strings.Contains(note, c.note) {
				t.Fatalf("gate=%v note=%q", got, note)
			}
		})
	}
}
