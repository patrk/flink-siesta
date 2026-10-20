package decide

import (
	"testing"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

// ADR 15: a stateless job whose Kafka source resumes from the consumer group's offsets.

func fromGroup(p policy.Policy) policy.Policy {
	p = withGroup(p)
	p.ResumeFromGroup, p.MaxSleep = true, 6*24*time.Hour
	return p
}

var (
	statelessRunning   = withMode2(running, "stateless")
	statelessSuspended = Live{SpecJobState: "suspended", UpgradeMode: "stateless", LifecycleState: "SUSPENDED"}
)

func expired(m map[string]string) Observation {
	return Observation{Snapshot: m, Known: true, Uncommitted: "group g has no committed offset for t partition 0"}
}

func TestResumeFromGroupOffsets(t *testing.T) {
	d := New(rp)
	idle := t0.Add(15 * 24 * time.Hour)
	cases := []struct {
		name   string
		policy policy.Policy
		prev   state.State
		live   Live
		obs    Observation
		now    time.Time
		want   Action
		held   string
	}{
		{"stateless without the opt-in is still refused", withGroup(auto), withSnap(state.Initial(t0), snap("1")), statelessRunning, lag(snap("1"), 0), idle, Refuse, ""},
		{"stateless, caught up, suspends", fromGroup(auto), withSnap(state.Initial(t0), snap("1")), statelessRunning, lag(snap("1"), 0), idle, Suspend, ""},
		{"stateless, records pending, stays awake", fromGroup(auto), withSnap(state.Initial(t0), snap("1")), statelessRunning, lag(snap("1"), 3), idle, None, "lag-pending"},
		{"stateless, lag unknown, stays awake", fromGroup(auto), withSnap(state.Initial(t0), snap("1")), statelessRunning, seen(snap("1")), idle, None, "lag-unknown"},
		{"input with offsets committed resumes", fromGroup(auto), suspendedState(), statelessSuspended, lag(snap("101"), 1), t0.Add(time.Hour), Resume, ""},
		{"input with offsets gone is held", fromGroup(auto), suspendedState(), statelessSuspended, expired(snap("101")), t0.Add(time.Hour), None, "offsets-missing"},
		{"input with lag unknown is held", fromGroup(auto), suspendedState(), statelessSuspended, seen(snap("101")), t0.Add(time.Hour), None, "lag-unknown"},
		{"max-sleep elapsed wakes without input", fromGroup(auto), suspendedState(), statelessSuspended, lag(snap("100"), 0), t0.Add(6*24*time.Hour + time.Second), Resume, ""},
		{"max-sleep not yet elapsed sleeps on", fromGroup(auto), suspendedState(), statelessSuspended, lag(snap("100"), 0), t0.Add(6 * 24 * time.Hour), None, ""},
		{"max-sleep 0 never wakes", withMaxSleep(fromGroup(auto), 0), suspendedState(), statelessSuspended, lag(snap("100"), 0), t0.Add(60 * 24 * time.Hour), None, ""},
		{"max-sleep with offsets already gone is held", fromGroup(auto), suspendedState(), statelessSuspended, expired(snap("100")), t0.Add(7 * 24 * time.Hour), None, "offsets-missing"},
		{"max-sleep is ignored when the job keeps its own state", fromGroup(auto), suspendedState(), suspended, lag(snap("100"), 0), t0.Add(60 * 24 * time.Hour), None, ""},
		{"the commit check is ignored when the job keeps its own state", fromGroup(auto), suspendedState(), suspended, expired(snap("101")), t0.Add(time.Hour), Resume, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := d.Decide(c.policy, c.prev, c.live, c.obs, c.now)
			if got.Action != c.want || got.Held != c.held {
				t.Fatalf("want %s held=%q, got %s held=%q (%s)", c.want, c.held, got.Action, got.Held, got.Reason)
			}
		})
	}
}

// A held resume keeps the input that asked for it: once the offsets are back, the job wakes
// without needing another record.
func TestHeldResumeKeepsTheInput(t *testing.T) {
	d := New(rp)
	pol := fromGroup(auto)
	got := d.Decide(pol, suspendedState(), statelessSuspended, expired(snap("101")), t0.Add(time.Hour))
	if got.Action != None || got.Next.Reason == ReasonIdle {
		t.Fatalf("setup: want a named hold, got %s (%s)", got.Action, got.Reason)
	}
	got = d.Decide(pol, got.Next, statelessSuspended, lag(snap("101"), 1), t0.Add(2*time.Hour))
	if got.Action != Resume || got.Reason != ReasonInputObserved {
		t.Fatalf("restored offsets must resume on the held input, got %s (%s)", got.Action, got.Reason)
	}
}

// A refresh wake keeps the last input's time: after min-awake, a caught-up job sleeps again
// without waiting a whole idle window, and it never sleeps before min-awake has passed.
func TestRefreshWakeSleepsAgainAfterMinAwake(t *testing.T) {
	d := New(rp)
	pol := fromGroup(auto)
	woke := t0.Add(6*24*time.Hour + time.Second)
	got := d.Decide(pol, suspendedState(), statelessSuspended, lag(snap("100"), 0), woke)
	if got.Action != Resume || !got.Next.LastActivityAt.Equal(t0) {
		t.Fatalf("want a refresh resume that keeps the last input's time, got %s at %v (%s)", got.Action, got.Next.LastActivityAt, got.Reason)
	}
	s := got.Next
	if got := d.Decide(pol, s, statelessRunning, lag(snap("100"), 0), woke.Add(30*time.Minute)); got.Action != None || got.Held != "min-awake" {
		t.Fatalf("within min-awake the job must stay up to commit, got %s held=%q", got.Action, got.Held)
	}
	if got := d.Decide(pol, s, statelessRunning, lag(snap("100"), 0), woke.Add(time.Hour+time.Minute)); got.Action != Suspend {
		t.Fatalf("after min-awake a caught-up job sleeps again, got %s (%s)", got.Action, got.Reason)
	}
}

func withMaxSleep(p policy.Policy, d time.Duration) policy.Policy {
	p.MaxSleep = d
	return p
}

// The state-space sweep again with the opt-in, so the new branches answer for every operator
// state: never a panic, never a resume the operator is not ready for, and never a resume with
// the group's offsets unknown or gone.
func TestResumeFromGroupAcrossTheStateSpace(t *testing.T) {
	d := New(rp)
	pol := withMaxSleep(fromGroup(auto), 0) // the sweep's clock is past any max-sleep
	now := t0.Add(30 * 24 * time.Hour)
	observations := map[string]Observation{
		"unknown":       unknown,
		"unchanged":     lag(snap("100"), 0),
		"moved":         lag(snap("101"), 1),
		"moved-nolag":   seen(snap("101")),
		"moved-expired": expired(snap("101")),
	}
	for _, lc := range lifecycles {
		for _, js := range jobStates {
			for _, sp := range specStates {
				for _, ph := range phases {
					for name, obs := range observations {
						for _, um := range upgradeModes {
							p := point{lc: lc, js: js, sp: sp, ph: ph, obs: name, um: um}
							prev := state.State{Phase: ph, Snapshot: snap("100"), LastActivityAt: t0, AwakeSince: t0, Generation: 1}
							if ph == state.Suspended {
								prev.SuspendedAt = t0
							}
							live := Live{SpecJobState: sp, UpgradeMode: um, JobState: js, LifecycleState: lc, Generation: 1}
							got := decideWithoutPanic(t, d, pol, prev, live, obs, now, p)
							if got.Reason == "" {
								t.Fatalf("%s: every decision carries a reason", p)
							}
							if got.Action == Resume && (lc != "SUSPENDED" || sp != "suspended" || ph != state.Suspended) {
								t.Fatalf("%s: resume only a completed suspend of ours", p)
							}
							if got.Action == Resume && um == "stateless" && name != "moved" {
								t.Fatalf("%s: a stateless resume needs the group's offsets", p)
							}
							if got.Action == Suspend && um == "stateless" && name != "unchanged" {
								t.Fatalf("%s: a stateless suspend needs the group caught up", p)
							}
						}
					}
				}
			}
		}
	}
}
