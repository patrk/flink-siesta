package decide

import (
	"fmt"
	"time"
)

// The reasons the controller writes on objects and in logs. One place, so that the README's
// promises about them can be checked against the code, and grep finds every use.
const (
	ReasonIdle             = "idle"
	ReasonActive           = "active"
	ReasonInputObserved    = "input observed"
	ReasonOffsetsUnknown   = "offsets unavailable"
	ReasonSuspensionOff    = "suspension disabled"
	ReasonSuspendedOutside = "suspended outside siesta"
	ReasonResumedOutside   = "resumed outside siesta"
	ReasonOperatorSuspends = "waiting for operator to finish suspending"
	ReasonOperatorRestarts = "restart left to the operator's health check"
	ReasonBudgetExhausted  = "restart budget exhausted"
)

func reasonOperatorBusy(lifecycle string) string   { return "operator busy: " + lifecycle }
func reasonNoInput(idleAfter time.Duration) string { return "no input for " + idleAfter.String() }
func reasonRefused(upgradeMode string) string {
	return "suspend refused: upgradeMode " + upgradeMode + " would lose the job's position"
}
func reasonLagUnknown(group string) string     { return "lag unknown for group " + group }
func reasonRecordsPending(group string) string { return "records pending for group " + group }
func reasonJobBusy(note string) string         { return "job busy: " + note }
func reasonJobUnknown(note string) string      { return "job unknown: " + note }
func reasonBackoffUntil(t time.Time) string {
	return "restart backoff until " + t.UTC().Format(time.RFC3339)
}
func reasonRestart(n int) string { return fmt.Sprintf("restart %d", n) }
func reasonRestarting(jobState string, n int) string {
	return fmt.Sprintf("job %s, restart %d", jobState, n)
}
