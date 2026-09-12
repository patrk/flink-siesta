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
	running   = Live{SpecJobState: "running", UpgradeMode: "savepoint", JobState: "RUNNING", LifecycleState: "STABLE"}
	suspended = Live{SpecJobState: "suspended", UpgradeMode: "savepoint", LifecycleState: "SUSPENDED"}
	rp        = RestartPolicy{MaxRestarts: 3, Window: 30 * time.Minute, BaseBackoff: time.Minute, Multiplier: 2, FailingAfter: 10 * time.Minute, Unrecoverable: []string{"UnknownTopicOrPartition"}}
)

func snap(v string) map[string]string { return map[string]string{"t-0": v} }

var unknown = Observation{}

func seen(m map[string]string) Observation { return Observation{Snapshot: m, Known: true} }

func lag(m map[string]string, pending int64) Observation {
	return Observation{Snapshot: m, Known: true, Pending: pending, LagKnown: true}
}

func withGroup(p policy.Policy) policy.Policy {
	p.ConsumerGroup = "g"
	return p
}

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
		obs    Observation
		now    time.Time
		want   Action
	}{
		{"unknown offsets never act", auto, state.Initial(t0), running, unknown, t0.Add(30 * 24 * time.Hour), None},
		{"first observation only records", auto, state.Initial(t0), running, seen(snap("100")), t0.Add(time.Minute), None},
		{"suspends after idle window", auto, withSnap(state.Initial(t0), snap("100")), running, seen(snap("100")), t0.Add(15 * 24 * time.Hour), Suspend},
		{"movement resets idle clock", auto, withSnap(state.Initial(t0), snap("1")), running, seen(snap("2")), t0.Add(30 * 24 * time.Hour), None},
		{"not before min-awake", auto, awoke(t0.Add(20 * 24 * time.Hour)), running, seen(snap("100")), t0.Add(20*24*time.Hour + 30*time.Minute), None},
		{"suspended, no movement", auto, suspendedState(), suspended, seen(snap("100")), t0.Add(24 * time.Hour), None},
		{"suspended, movement resumes", auto, suspendedState(), suspended, seen(snap("101")), t0.Add(24 * time.Hour), Resume},
		{"mode off resumes without traffic", withMode(auto, policy.ModeOff), suspendedState(), suspended, unknown, t0, Resume},
		{"operator busy is hands off", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "RUNNING", LifecycleState: "UPGRADING"}, seen(snap("1")), t0, None},
		{"failed job restarts", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "FAILED", LifecycleState: "STABLE"}, seen(snap("1")), t0.Add(time.Minute), Restart},
		{"unrecoverable error never restarts", auto, state.Initial(t0), Live{SpecJobState: "running", JobState: "FAILED", LifecycleState: "STABLE", ReconcileError: "org.apache.kafka.common.errors.UnknownTopicOrPartitionException"}, seen(snap("1")), t0, MarkUnrecoverable},
		{"not before the operator is STABLE", auto, withSnap(state.Initial(t0), snap("1")), Live{SpecJobState: "running", JobState: "RUNNING", LifecycleState: "DEPLOYED"}, seen(snap("1")), t0.Add(15 * 24 * time.Hour), None},
		{"no resume while the operator is still suspending", auto, suspendedState(), Live{SpecJobState: "suspended", JobState: "RUNNING", LifecycleState: "STABLE"}, seen(snap("101")), t0.Add(24 * time.Hour), None},
		{"mode off waits for the operator too", withMode(auto, policy.ModeOff), suspendedState(), Live{SpecJobState: "suspended", JobState: "RUNNING", LifecycleState: "STABLE"}, unknown, t0, None},
		{"stateless upgradeMode is refused", auto, withSnap(state.Initial(t0), snap("1")), withMode2(running, "stateless"), seen(snap("1")), t0.Add(15 * 24 * time.Hour), Refuse},
		{"last-state upgradeMode is fine", auto, withSnap(state.Initial(t0), snap("1")), withMode2(running, "last-state"), seen(snap("1")), t0.Add(15 * 24 * time.Hour), Suspend},
		{"with a consumer group, pending records block suspend", withGroup(auto), withSnap(state.Initial(t0), snap("1")), running, lag(snap("1"), 42), t0.Add(15 * 24 * time.Hour), None},
		{"with a consumer group, unknown lag blocks suspend", withGroup(auto), withSnap(state.Initial(t0), snap("1")), running, seen(snap("1")), t0.Add(15 * 24 * time.Hour), None},
		{"with a consumer group, caught up suspends", withGroup(auto), withSnap(state.Initial(t0), snap("1")), running, lag(snap("1"), 0), t0.Add(15 * 24 * time.Hour), Suspend},
		{"only suspends a RUNNING job", auto, withSnap(state.Initial(t0), snap("1")), Live{SpecJobState: "running", JobState: "RESTARTING", LifecycleState: "STABLE"}, seen(snap("1")), t0.Add(15 * 24 * time.Hour), Restart},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Decide(c.policy, c.prev, c.live, c.obs, c.now)
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
		got := d.Decide(auto, s, failed, seen(map[string]string{}), now)
		if got.Action != Restart {
			t.Fatalf("restart %d: want restart, got %s (%s)", i, got.Action, got.Reason)
		}
		s, now = got.Next, got.Next.Restarts.NextAfter
	}
	if got := d.Decide(auto, s, failed, seen(map[string]string{}), now.Add(time.Second)); got.Action != MarkUnrecoverable {
		t.Fatalf("want mark-unrecoverable, got %s", got.Action)
	}
}

func withSnap(s state.State, m map[string]string) state.State {
	s.Snapshot = m
	return s
}

func withMode2(l Live, upgradeMode string) Live {
	l.UpgradeMode = upgradeMode
	return l
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

func TestSpecChangeClearsUnrecoverable(t *testing.T) {
	d := New(rp)
	stuck := state.State{Phase: state.Unrecoverable, Snapshot: snap("1"), Reason: "restart budget exhausted", Generation: 3}
	same := d.Decide(auto, stuck, withGen(running, 3), seen(snap("1")), t0)
	if same.Next.Phase != state.Unrecoverable {
		t.Fatalf("same generation must stay unrecoverable, got %s", same.Next.Phase)
	}
	edited := d.Decide(auto, stuck, withGen(running, 4), seen(snap("1")), t0)
	if edited.Action != None || edited.Next.Phase != state.Active || edited.Next.Restarts.Count != 0 {
		t.Fatalf("a new generation must clear unrecoverable and the budget without acting, got %+v", edited)
	}
	if !edited.Next.AwakeSince.Equal(t0) || !edited.Next.LastActivityAt.Equal(t0) {
		t.Fatalf("a new generation must restart the idle and min-awake clocks, got %+v", edited.Next)
	}
}

func withGen(l Live, g int64) Live {
	l.Generation = g
	return l
}

func TestResumeLatencyIsReportedOnceRunning(t *testing.T) {
	d := New(rp)
	resumed := d.Decide(auto, suspendedState(), suspended, seen(snap("101")), t0)
	if resumed.Action != Resume || !resumed.Next.ResumedAt.Equal(t0) {
		t.Fatalf("resume must stamp ResumedAt, got %+v", resumed)
	}
	notYet := d.Decide(auto, resumed.Next, Live{SpecJobState: "running", UpgradeMode: "savepoint", JobState: "RECONCILING", LifecycleState: "DEPLOYED"}, seen(snap("101")), t0.Add(30*time.Second))
	if notYet.ResumedAfter != 0 || notYet.Next.ResumedAt.IsZero() {
		t.Fatalf("latency must not be reported before RUNNING, got %+v", notYet)
	}
	up := d.Decide(auto, notYet.Next, running, seen(snap("101")), t0.Add(75*time.Second))
	if up.ResumedAfter != 75*time.Second || !up.Next.ResumedAt.IsZero() {
		t.Fatalf("latency must be reported once and cleared, got %+v", up)
	}
}
