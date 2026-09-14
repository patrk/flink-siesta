package policy

import (
	"strings"
	"time"
)

type Mode string

const (
	ModeAuto Mode = "auto"
	ModeOff  Mode = "off"
)

type Policy struct {
	Mode      Mode
	Sources   []string      // Kafka topic names
	IdleAfter time.Duration // no input for this long -> suspend
	MinAwake  time.Duration // never suspend sooner than this after a resume or restart
	Restart   bool          // restart-on-failure enabled
	// ConsumerGroup, when set, makes "idle" also require that the group has consumed everything
	// (lag zero). Empty means only "no new input" is checked.
	ConsumerGroup string
	// IdleFromJob, from `idle: job`, adds the running job's own view as a gate: it may block a
	// suspend while it still has records to emit or emitted one recently, never cause one.
	IdleFromJob bool
	// Problems lists values that were missing or unparsable. A policy with problems is still
	// "ours" (mode is set) but must not be acted on; the reconciler reports them once.
	Problems []string
}

// Read returns ok=false when the object carries no policy at all (not ours to manage).
func Read(prefix string, ann map[string]string) (Policy, bool) {
	mode, has := ann[prefix+"/mode"]
	if !has {
		return Policy{}, false
	}
	p := Policy{
		Mode:          ModeAuto,
		Restart:       !strings.EqualFold(ann[prefix+"/restart"], "off"),
		ConsumerGroup: strings.TrimSpace(ann[prefix+"/consumer-group"]),
	}
	switch {
	case strings.EqualFold(mode, "off"):
		p.Mode = ModeOff
	case strings.EqualFold(mode, "auto"):
	default:
		p.Problems = append(p.Problems, prefix+"/mode must be auto or off, got "+mode)
	}
	switch idle := strings.TrimSpace(ann[prefix+"/idle"]); {
	case idle == "":
	case strings.EqualFold(idle, "job"):
		p.IdleFromJob = true
	default:
		p.Problems = append(p.Problems, prefix+"/idle must be job or absent, got "+idle)
	}
	p.IdleAfter = p.duration(prefix+"/idle-after", ann, 14*24*time.Hour)
	p.MinAwake = p.duration(prefix+"/min-awake", ann, time.Hour)
	for _, s := range strings.Split(ann[prefix+"/sources"], ",") {
		if s = strings.TrimSpace(s); s != "" {
			p.Sources = append(p.Sources, s)
		}
	}
	if len(p.Sources) == 0 {
		p.Problems = append(p.Problems, prefix+"/sources is required: comma-separated topic names")
	}
	// Reserved for a per-deployment Kafka cluster. The name is part of the contract already, so
	// that adding it later is not a breaking change; until then it is refused, not ignored.
	if _, has := ann[prefix+"/bootstrap-servers"]; has {
		p.Problems = append(p.Problems, prefix+"/bootstrap-servers is reserved and not implemented yet; the controller's Kafka cluster is set at install time")
	}
	return p, true
}

// duration parses an annotation, recording a problem instead of silently using the default.
func (p *Policy) duration(key string, ann map[string]string, dflt time.Duration) time.Duration {
	raw, has := ann[key]
	if !has || strings.TrimSpace(raw) == "" {
		return dflt
	}
	d, ok := parseDuration(raw)
	if !ok {
		p.Problems = append(p.Problems, key+" is not a duration: "+raw)
		return dflt
	}
	return d
}

// parseDuration accepts Go durations ("30m", "2h") plus a "d" suffix, which time.ParseDuration lacks.
func parseDuration(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		d, err := time.ParseDuration(strings.TrimSuffix(s, "d") + "h")
		return d * 24, err == nil
	}
	d, err := time.ParseDuration(s)
	return d, err == nil
}
