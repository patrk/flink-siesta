// Package store keeps the controller's memory in a ConfigMap next to each FlinkDeployment.
// The deployment itself carries only what a human wants from kubectl describe: the policy
// annotations, the state and the reason. Everything the controller needs to remember lives
// here, owned by the deployment so it is garbage-collected with it, and watched by nobody,
// so writing it wakes no other controller. See ADR 10.
package store

import (
	"context"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patrk/flink-siesta/internal/state"
)

type Store struct {
	Client client.Client // writes
	Reader client.Reader // uncached reads: we do not want an informer on ConfigMaps
	Prefix string
}

// Name of the ConfigMap that holds a deployment's state.
func Name(deployment string) string { return "siesta-" + deployment }

// Load returns the state and whether any was found. A missing ConfigMap falls back to the
// deployment's own annotations, which is how state written by older versions is migrated.
func (s Store) Load(ctx context.Context, fd *unstructured.Unstructured) (state.State, bool, error) {
	var cm corev1.ConfigMap
	err := s.Reader.Get(ctx, types.NamespacedName{Namespace: fd.GetNamespace(), Name: Name(fd.GetName())}, &cm)
	switch {
	case err == nil:
		st, ok := state.FromData(cm.Data)
		return st, ok, nil
	case client.IgnoreNotFound(err) == nil:
		st, ok := state.Read(s.Prefix, fd.GetAnnotations())
		return st, ok, nil
	default:
		return state.State{}, false, fmt.Errorf("load state: %w", err)
	}
}

// Save writes the state, creating the ConfigMap on first use. Unchanged data is not written.
func (s Store) Save(ctx context.Context, fd *unstructured.Unstructured, st state.State) error {
	data := st.Data()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: fd.GetNamespace(), Name: Name(fd.GetName())}}
	err := s.Reader.Get(ctx, client.ObjectKeyFromObject(cm), cm)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("save state: %w", err)
	}
	if err != nil { // not found: create, owned by the deployment
		cm.Data = data
		cm.Labels = map[string]string{"app.kubernetes.io/managed-by": "flink-siesta"}
		cm.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: fd.GetAPIVersion(), Kind: fd.GetKind(), Name: fd.GetName(), UID: fd.GetUID(),
			Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
		}}
		return s.Client.Create(ctx, cm, client.FieldOwner("siesta"))
	}
	if maps.Equal(cm.Data, data) {
		return nil
	}
	patch := client.MergeFrom(cm.DeepCopy())
	cm.Data = data
	return s.Client.Patch(ctx, cm, patch, client.FieldOwner("siesta"))
}
