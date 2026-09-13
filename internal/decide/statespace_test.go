package decide

import (
	"testing"
	"time"

	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/state"
)

// The operator's state space is small and finite. Rather than adding a guard per incident, this
// test walks every combination and checks the invariants that must hold everywhere. If a new
// operator version adds a lifecycle state, add it here and the decider must answer for it.
var (
	lifecycles = []string{"", "CREATED", "DEPLOYED", "STABLE", "SUSPENDED", "UPGRADING", "ROLLING_BACK", "ROLLED_BACK", "FAILED"}
	jobStates  = []string{"", "CREATED", "RECONCILING", "RUNNING", "RESTARTING", "FAILED", "FINISHED", "CANCELED"}
	specStates = []string{"running", "suspended"}
	phases     = []state.Phase{state.Active, state.Suspended, state.Unrecoverable}
)

func TestEveryOperatorStateHasADefinedAnswer(t *testing.T) {
	d := New(rp)
	now := t0.Add(30 * 24 * time.Hour) // long past every window, so time never blocks an action
	observations := map[string]Observation{
		"unknown":   unknown,
		"unchanged": seen(snap("100")),
		"moved":     seen(snap("101")),
	}
	checked := 0
	for _, lc := range lifecycles {
		for _, js := range jobStates {
			for _, sp := range specStates {
				for _, ph := range phases {
					for name, obs := range observations {
						prev := state.State{Phase: ph, Snapshot: snap("100"), LastActivityAt: t0, AwakeSince: t0, Generation: 1}
						if ph == state.Suspended {
							prev.SuspendedAt = t0
						}
						live := Live{SpecJobState: sp, UpgradeMode: "savepoint", JobState: js, LifecycleState: lc, Generation: 1}
						var got Decision
						func() {
							defer func() {
								if r := recover(); r != nil {
									t.Fatalf("panic at lifecycle=%q job=%q spec=%s phase=%s obs=%s: %v", lc, js, sp, ph, name, r)
								}
							}()
							got = d.Decide(auto, prev, live, obs, now)
						}()
						checked++
						where := "lifecycle=" + lc + " job=" + js + " spec=" + sp + " phase=" + string(ph) + " obs=" + name

						if got.Reason == "" {
							t.Fatalf("%s: every decision carries a reason", where)
						}
						busy := lc == "UPGRADING" || lc == "ROLLING_BACK"
						if busy && got.Action != None {
							t.Fatalf("%s: never act while the operator is mid-change, got %s", where, got.Action)
						}
						maySuspend := lc == "STABLE" && js == "RUNNING" && sp == "running" && ph == state.Active && name != "unknown"
						mayResume := lc == "SUSPENDED" && sp == "suspended" && ph == state.Suspended && name == "moved"
						mayRestart := js == "FAILED" || lc == "FAILED" || js == "RESTARTING"
						if got.Action == Suspend && !maySuspend {
							t.Fatalf("%s: suspend is only allowed from STABLE+RUNNING, active, observed", where)
						}
						if got.Action == Resume && !mayResume {
							t.Fatalf("%s: resume is only allowed from SUSPENDED after input", where)
						}
						if got.Action == Restart && !mayRestart {
							t.Fatalf("%s: restart is only allowed on a failing job", where)
						}
						if name == "unknown" && got.Action != None {
							t.Fatalf("%s: unknown never acts, got %s", where, got.Action)
						}
						if ph == state.Unrecoverable && got.Action != None {
							t.Fatalf("%s: unrecoverable waits for a human, got %s", where, got.Action)
						}
					}
				}
			}
		}
	}
	t.Logf("%d combinations checked", checked)
}

// mode off must never suspend, and must resume anything we suspended, regardless of traffic.
func TestModeOffAcrossTheStateSpace(t *testing.T) {
	d := New(rp)
	off := withMode(auto, policy.ModeOff)
	now := t0.Add(30 * 24 * time.Hour)
	for _, lc := range lifecycles {
		for _, js := range jobStates {
			for _, sp := range specStates {
				prev := state.State{Phase: state.Active, Snapshot: snap("100"), LastActivityAt: t0, AwakeSince: t0}
				live := Live{SpecJobState: sp, UpgradeMode: "savepoint", JobState: js, LifecycleState: lc}
				if got := d.Decide(off, prev, live, seen(snap("100")), now); got.Action == Suspend {
					t.Fatalf("mode off must never suspend (lifecycle=%s job=%s spec=%s)", lc, js, sp)
				}
			}
		}
	}
}
