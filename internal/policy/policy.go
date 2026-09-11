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
}

func Read(prefix string, ann map[string]string) (Policy, bool) {
	mode, has := ann[prefix+"/mode"]
	if !has {
		return Policy{}, false
	}
	p := Policy{
		Mode:      ModeAuto,
		IdleAfter: parseDuration(ann[prefix+"/idle-after"], 14*24*time.Hour),
		MinAwake:  parseDuration(ann[prefix+"/min-awake"], time.Hour),
		Restart:   !strings.EqualFold(ann[prefix+"/restart"], "off"),
	}
	if strings.EqualFold(mode, "off") {
		p.Mode = ModeOff
	}
	for _, s := range strings.Split(ann[prefix+"/sources"], ",") {
		if s = strings.TrimSpace(s); s != "" {
			p.Sources = append(p.Sources, s)
		}
	}
	return p, len(p.Sources) > 0
}

func parseDuration(s string, dflt time.Duration) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return dflt
	}
	if strings.HasSuffix(s, "d") {
		if d, err := time.ParseDuration(strings.TrimSuffix(s, "d") + "h"); err == nil {
			return d * 24
		}
		return dflt
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return dflt
}
