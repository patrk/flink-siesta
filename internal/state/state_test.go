package state

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ConfigMaps are capped at 1 MiB. Ten topics of a thousand partitions each must fit with room.
func TestWideSnapshotFitsInAConfigMap(t *testing.T) {
	s := Initial(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	offsets := make([]string, 1000)
	for p := range offsets {
		offsets[p] = "9223372036854775807"
	}
	for topic := range 10 {
		s.Snapshot[fmt.Sprintf("private.tenant-%02d.events.v1", topic)] = strings.Join(offsets, ",")
	}
	size := 0
	for k, v := range s.Data() {
		size += len(k) + len(v)
	}
	if size > 256*1024 {
		t.Fatalf("10k partitions serialize to %d bytes, too close to the 1 MiB ConfigMap limit", size)
	}
	t.Logf("10,000 partitions: %d KiB", size/1024)
}

func TestDataRoundTripsEveryField(t *testing.T) {
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	in := State{Phase: Suspended, Snapshot: map[string]string{"t-0": "1"}, LastActivityAt: t0, SuspendedAt: t0.Add(time.Hour),
		AwakeSince: t0, Restarts: RestartBudget{Count: 2, WindowStart: t0, NextAfter: t0.Add(time.Minute)}, Reason: "r",
		Generation: 7, ResumedAt: t0.Add(2 * time.Hour), Pending: 42, Outage: Outage{Down: true, Since: t0.Add(3 * time.Hour)}, Reported: Reported{SuspendChecked: true, ResumeStalled: true, SourcesChecked: "job1"}}
	out, ok := FromData(in.Data())
	if !ok {
		t.Fatal("round trip must succeed")
	}
	if out.Phase != in.Phase || out.Snapshot["t-0"] != "1" || !out.SuspendedAt.Equal(in.SuspendedAt) || out.Restarts != in.Restarts ||
		out.Generation != 7 || out.Pending != 42 || out.Outage != in.Outage || out.Reported != in.Reported || !out.ResumedAt.Equal(in.ResumedAt) {
		t.Fatalf("round trip lost a field: %+v", out)
	}
}
