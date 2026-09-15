package policy

import (
	"testing"
	"time"
)

func TestReadReportsProblemsInsteadOfDefaultingSilently(t *testing.T) {
	const p = "siesta.flink.io"
	cases := []struct {
		name     string
		ann      map[string]string
		ours     bool
		problems int
		idle     time.Duration
	}{
		{"not ours without mode", map[string]string{p + "/sources": "t"}, false, 0, 0},
		{"complete", map[string]string{p + "/mode": "auto", p + "/sources": "a, b", p + "/idle-after": "2d"}, true, 0, 48 * time.Hour},
		{"missing sources", map[string]string{p + "/mode": "auto"}, true, 1, 14 * 24 * time.Hour},
		{"garbage duration", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/idle-after": "2 weeks"}, true, 1, 14 * 24 * time.Hour},
		{"bad mode", map[string]string{p + "/mode": "maybe", p + "/sources": "t"}, true, 1, 14 * 24 * time.Hour},
		{"idle from the job", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/idle": "job"}, true, 0, 14 * 24 * time.Hour},
		{"bad idle", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/idle": "broker"}, true, 1, 14 * 24 * time.Hour},
		{"restart off", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/restart": "off"}, true, 0, 14 * 24 * time.Hour},
		{"bad restart", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/restart": "sometimes"}, true, 1, 14 * 24 * time.Hour},
		{"sources auto", map[string]string{p + "/mode": "auto", p + "/sources": "auto"}, true, 0, 14 * 24 * time.Hour},
		{"bootstrap-servers is reserved", map[string]string{p + "/mode": "auto", p + "/sources": "t", p + "/bootstrap-servers": "b:9092"}, true, 1, 14 * 24 * time.Hour},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pol, ours := Read(p, c.ann)
			if ours != c.ours || len(pol.Problems) != c.problems || (c.ours && pol.IdleAfter != c.idle) {
				t.Fatalf("ours=%v problems=%v idle=%v", ours, pol.Problems, pol.IdleAfter)
			}
		})
	}
}
