package controller

import (
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func reconcileRequest(k types.NamespacedName) ctrl.Request { return ctrl.Request{NamespacedName: k} }
