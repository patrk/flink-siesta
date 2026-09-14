package controller

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/patrk/flink-siesta/internal/state"
)

type patcher struct {
	client.Client
	Reader   client.Reader // uncached, for the fresh read after a conflict
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

// restart writes the nonce the decider recorded in the budget, so memory and object agree on
// which restart the budget has counted, even if the save after this patch is lost.
func (p patcher) restart(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string) error {
	err := p.merge(ctx, fd, s, map[string]any{"restartNonce": s.Restarts.LastNonce})
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

// markUnrecoverable records the state and raises a Warning: this deployment needs a human.
func (p patcher) markUnrecoverable(ctx context.Context, fd *unstructured.Unstructured, s state.State, reason string) error {
	err := p.annotate(ctx, fd, s)
	if p.Recorder != nil {
		p.Recorder.Eventf(fd, nil, corev1.EventTypeWarning, "Unrecoverable", "MarkUnrecoverable", "%s", reason)
	}
	return err
}

// clear removes our annotations from an object that is no longer ours.
func (p patcher) clear(ctx context.Context, fd *unstructured.Unstructured) error {
	return p.merge(ctx, fd, state.State{}, nil)
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
	// The resourceVersion makes the merge patch conditional: a human edit between our cached
	// read and this write is a conflict, retried on the next tick against the new object.
	body := map[string]any{"metadata": map[string]any{"annotations": ann, "resourceVersion": fd.GetResourceVersion()}}
	if spec != nil {
		body["spec"] = spec
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	// A named field manager lets GitOps tools ignore what we own by manager, not by path.
	err = p.Patch(ctx, fd, client.RawPatch(types.MergePatchType, raw), client.FieldOwner("siesta"))
	if !apierrors.IsConflict(err) || p.Reader == nil {
		return err
	}
	// The operator writes status every few seconds, so the cached resourceVersion is often
	// behind. Read the object fresh and retry once, unless the one field we are about to set was
	// changed by someone else in the meantime: that edit wins, and the next tick sees it.
	fresh := fd.DeepCopy()
	if err := p.Reader.Get(ctx, client.ObjectKeyFromObject(fd), fresh); err != nil {
		return err
	}
	was, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state")
	is, _, _ := unstructured.NestedString(fresh.Object, "spec", "job", "state")
	if spec != nil && was != is {
		return fmt.Errorf("spec.job.state changed from %q to %q while deciding: %w", was, is, err)
	}
	body["metadata"].(map[string]any)["resourceVersion"] = fresh.GetResourceVersion()
	if raw, err = json.Marshal(body); err != nil {
		return err
	}
	return p.Patch(ctx, fresh, client.RawPatch(types.MergePatchType, raw), client.FieldOwner("siesta"))
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
