package decide

import (
	"testing"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

var (
	t0        = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	auto      = policy.Policy{Mode: policy.ModeAuto, Sources: []string{"t"}, IdleAfter: 14 * 24 * time.Hour, MinAwake: time.Hour, Restart: true}
	running   = Live{SpecJobState: "running", JobState: "RUNNING", LifecycleState: "STABLE"}
	suspended = Live{SpecJobState: "suspended", LifecycleState: "SUSPENDED"}
	rp        = RestartPolicy{MaxRestarts: 3, Window: 30 * time.Minute, BaseBackoff: time.Minute, Multiplier: 2, FailingAfter: 10 * time.Minute, Unrecoverable: []string{"UnknownTopicOrPartition"}}
)

func snap(v string) map[string]string { return map[string]string{"t-0": v} }

func suspendedState() state.State {
	return state.State{Phase: state.Suspended, Snapshot: snap("100"), LastActivityAt: t0, SuspendedAt: t0, AwakeSince: t0, Reason: "idle"}
}

func TestDecide(t *testing.T) {
	d := New(rp)
	cases := []struct {
		name   string
		policy policy.Policy
		prev   state.State
		live   Live
		snap   map[string]string
		known  bool
		now    time.Time
		want   Action
	}{
		{"unknown offsets never act", auto, state.Initial(t0), running, nil, false, t0.Add(30 * 24 * time.Hour), None},
		{"first observation only records", auto, state.Initial(t0), running, snap("100"), true, t0.Add(time.Minute), None},
		{"suspends after idle window", auto, withSnap(state.Initial(t0), snap("100")), running, snap("100"), true, t0.Add(15 * 24 * time.Hour), Suspend},
		{"movement resets idle clock", auto, withSnap(state.Initial(t0), snap("1")), running, snap("2"), true, t0.Add(30 * 24 * time.Hour), None},
		{"not before min-awake", auto, awoke(t0.Add(20 * 24 * time.Hour)), running, snap("100"), true, t0.Add(20*24*time.Hour + 30*time.Minute), None},
		{"suspended, no movement", auto, suspendedState(), suspended, snap("100"), true, t0.Add(24 * time.Hour), None},
		{"suspended, movement resumes", auto, suspendedState(), suspended, snap("101"), true, t0.Add(24 * time.Hour), Resume},
		{"mode off resumes without traffic", withMode(auto, policy.ModeOff), suspendedState(), suspended, nil, false, t0, Resume},
		{"operator busy is hands off", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "RUNNING", LifecycleState: "UPGRADING"}, snap("1"), true, t0, None},
		{"failed job restarts", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "FAILED", LifecycleState: "STABLE"}, snap("1"), true, t0.Add(time.Minute), Restart},
		{"unrecoverable error never restarts", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "FAILED", LifecycleState: "STABLE", ReconcileError: "org.apache.kafka.common.errors.UnknownTopicOrPartitionException"}, snap("1"), true, t0, MarkUnrecoverable},
		{"not before the operator is STABLE", auto, withSnap(state.Initial(t0), snap("1")), Live{SpecJobState: "running", JobState: "RUNNING", LifecycleState: "DEPLOYED"}, snap("1"), true, t0.Add(15 * 24 * time.Hour), None},
		{"no resume while the operator is still suspending", auto, suspendedState(), Live{SpecJobState: "suspended", JobState: "RUNNING", LifecycleState: "STABLE"}, snap("101"), true, t0.Add(24 * time.Hour), None},
		{"mode off waits for the operator too", withMode(auto, policy.ModeOff), suspendedState(), Live{SpecJobState: "suspended", JobState: "RUNNING", LifecycleState: "STABLE"}, nil, false, t0, None},
		{"only suspends a RUNNING job", auto, withSnap(state.Initial(t0), snap("1")), Live{SpecJobState: "running", JobState: "RESTARTING", LifecycleState: "STABLE"}, snap("1"), true, t0.Add(15 * 24 * time.Hour), Restart},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Decide(c.policy, c.prev, c.live, c.snap, c.known, c.now)
			if got.Action != c.want {
				t.Fatalf("want %s, got %s (%s)", c.want, got.Action, got.Reason)
			}
		})
	}
}

func TestRestartBudgetThenUnrecoverable(t *testing.T) {
	d := New(rp)
	failed := Live{SpecJobState: "running", JobState: "FAILED", LifecycleState: "STABLE"}
	s, now := state.Initial(t0), t0
	for i := 1; i <= 3; i++ {
		now = now.Add(5 * time.Minute)
		got := d.Decide(auto, s, failed, map[string]string{}, true, now)
		if got.Action != Restart {
			t.Fatalf("restart %d: want restart, got %s (%s)", i, got.Action, got.Reason)
		}
		s, now = got.Next, got.Next.Restarts.NextAfter
	}
	if got := d.Decide(auto, s, failed, map[string]string{}, true, now.Add(time.Second)); got.Action != MarkUnrecoverable {
		t.Fatalf("want mark-unrecoverable, got %s", got.Action)
	}
}

func withSnap(s state.State, m map[string]string) state.State {
	s.Snapshot = m
	return s
}

func withMode(p policy.Policy, m policy.Mode) policy.Policy {
	p.Mode = m
	return p
}

func awoke(at time.Time) state.State {
	s := state.Initial(t0)
	s.Snapshot, s.LastActivityAt, s.AwakeSince = snap("100"), t0, at
	return s
}
