package controller

import (
	"context"
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/patrk/flink-siesta/internal/state"
)

type patcher struct {
	client.Client
	Prefix   string
	Recorder recorder.EventRecorder
}

func (p patcher) suspend(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string) error {
	err := p.merge(ctx, fd, s, map[string]any{"job": map[string]any{"state": "suspended"}})
	p.event(fd, err, "Suspended", reason)
	return err
}

func (p patcher) resume(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string) error {
	err := p.merge(ctx, fd, s, map[string]any{"job": map[string]any{"state": "running"}})
	p.event(fd, err, "Resumed", reason)
	return err
}

func (p patcher) restart(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string, now time.Time) error {
	err := p.merge(ctx, fd, s, map[string]any{"restartNonce": now.UnixMilli()})
	p.event(fd, err, "Restarted", reason)
	return err
}

// refuse records why the controller did not act and raises a Warning so it shows in kubectl describe.
func (p patcher) refuse(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string) error {
	err := p.annotate(ctx, fd, s)
	if p.Recorder != nil {
		p.Recorder.Eventf(fd, nil, corev1.EventTypeWarning, "Refused", "Suspend", "%s", reason)
	}
	return err
}

func (p patcher) annotate(ctx context.Context, fd *unstructured.Unstructured, s state.State) error {
	want := s.Annotations(p.Prefix)
	have := fd.GetAnnotations()
	unchanged := true
	for k, v := range want {
		if have[k] != v {
			unchanged = false
			break
		}
	}
	if unchanged {
		return nil
	}
	return p.merge(ctx, fd, s, nil)
}

func (p patcher) merge(ctx context.Context, fd *unstructured.Unstructured, s state.State, spec map[string]any) error {
	ann := map[string]any{}
	for k, v := range s.Annotations(p.Prefix) {
		if v == "" {
			ann[k] = nil
		} else {
			ann[k] = v
		}
	}
	body := map[string]any{"metadata": map[string]any{"annotations": ann}}
	if spec != nil {
		body["spec"] = spec
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return p.Patch(ctx, fd, client.RawPatch(types.MergePatchType, raw))
}

func (p patcher) event(fd *unstructured.Unstructured, err error, action, note string) {
	if p.Recorder == nil {
		return
	}
	if err != nil {
		p.Recorder.Eventf(fd, nil, corev1.EventTypeWarning, action+"Failed", action, "%s", err.Error())
		return
	}
	p.Recorder.Eventf(fd, nil, corev1.EventTypeNormal, action, action, "%s", note)
}
