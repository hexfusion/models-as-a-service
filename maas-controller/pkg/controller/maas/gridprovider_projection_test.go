/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package maas

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const targetNS = "models-as-a-service"

// gridSite builds a cluster-scoped grid GridSite fixture with the given egress
// address, lifecycle phase, and inference capability.
func gridSite(name, address, phase string, inference bool) *unstructured.Unstructured {
	s := &unstructured.Unstructured{}
	s.SetGroupVersionKind(gridSiteGVK)
	s.SetName(name)
	if address != "" {
		_ = unstructured.SetNestedField(s.Object, address, "spec", "egress", "address")
	}
	_ = unstructured.SetNestedField(s.Object, phase, "status", "phase")
	_ = unstructured.SetNestedField(s.Object, inference, "status", "capabilities", "inference")
	return s
}

func TestProjectExternalResources_ActiveInferenceSite(t *testing.T) {
	in := gridSite("site-d", "site-d.grid.internal:8443", "Active", true)

	out, err := projectExternalResources(in, targetNS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected exactly one ExternalProvider, got %d objects", len(out))
	}

	ep := out[0]
	if got := ep.GetKind(); got != "ExternalProvider" {
		t.Fatalf("out[0] kind = %q, want ExternalProvider", got)
	}
	if got := ep.GetName(); got != "site-d" {
		t.Fatalf("ExternalProvider name = %q, want site-d", got)
	}
	if got := ep.GetNamespace(); got != targetNS {
		t.Fatalf("ExternalProvider namespace = %q, want %q", got, targetNS)
	}
	// egress address has no scheme, so the projection adds https for the mTLS egress.
	if got, _, _ := unstructured.NestedString(ep.Object, "spec", "endpoint"); got != "https://site-d.grid.internal:8443" {
		t.Fatalf("endpoint = %q, want https://site-d.grid.internal:8443", got)
	}
	if got, _, _ := unstructured.NestedString(ep.Object, "spec", "auth", "type"); got != "mtls" {
		t.Fatalf("auth.type = %q, want mtls", got)
	}
	if got, _, _ := unstructured.NestedString(ep.Object, "spec", "auth", "secretRef", "name"); got != "site-d-grid-identity" {
		t.Fatalf("secretRef.name = %q, want site-d-grid-identity", got)
	}
}

func TestProjectExternalResources_NotActiveProjectsNothing(t *testing.T) {
	in := gridSite("site-e", "site-e.grid.internal:8443", "Connecting", true)
	out, err := projectExternalResources(in, targetNS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("a non-Active site must project nothing, got %d objects", len(out))
	}
}

func TestProjectExternalResources_NoInferenceProjectsNothing(t *testing.T) {
	in := gridSite("site-f", "site-f.grid.internal:8443", "Active", false)
	out, err := projectExternalResources(in, targetNS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("a site not offering inference must project nothing, got %d objects", len(out))
	}
}

func TestProjectExternalResources_ActiveWithoutEgressFails(t *testing.T) {
	in := gridSite("site-g", "", "Active", true)
	if _, err := projectExternalResources(in, targetNS); err == nil {
		t.Fatal("expected an error when an Active inference site has no egress address, got nil")
	}
}

// newProjectionReconciler builds a reconciler backed by a fake client that can
// track and list the unstructured catalog kinds.
func newProjectionReconciler() *GridProviderReconciler {
	scheme := runtime.NewScheme()
	for _, gvk := range []schema.GroupVersionKind{externalProviderGVK, gridSiteGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk
		listGVK.Kind += "List"
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	return &GridProviderReconciler{Client: cl, Scheme: scheme, TargetNamespace: targetNS}
}

func countKind(t *testing.T, r *GridProviderReconciler, gvk schema.GroupVersionKind) int {
	t.Helper()
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)
	if err := r.List(context.Background(), list); err != nil {
		t.Fatalf("list %s: %v", gvk.Kind, err)
	}
	return len(list.Items)
}

func TestSync_CreatesThenPrunesWhenSiteLeaves(t *testing.T) {
	r := newProjectionReconciler()
	ctx := context.Background()

	// create: an Active inference site projects to one ExternalProvider
	if err := r.sync(ctx, gridSite("site-d", "site-d.grid.internal:8443", "Active", true)); err != nil {
		t.Fatalf("sync (create): %v", err)
	}
	if got := countKind(t, r, externalProviderGVK); got != 1 {
		t.Fatalf("ExternalProvider count = %d, want 1", got)
	}

	// update: the site goes Unreachable — the desired set is empty, so the
	// projection must prune the ExternalProvider it created.
	if err := r.sync(ctx, gridSite("site-d", "site-d.grid.internal:8443", "Unreachable", true)); err != nil {
		t.Fatalf("sync (update): %v", err)
	}
	if got := countKind(t, r, externalProviderGVK); got != 0 {
		t.Fatalf("after the site left, ExternalProvider count = %d, want 0", got)
	}

	gone := &unstructured.Unstructured{}
	gone.SetGroupVersionKind(externalProviderGVK)
	if err := r.Get(ctx, client.ObjectKey{Namespace: targetNS, Name: "site-d"}, gone); !apierrors.IsNotFound(err) {
		t.Fatalf("site-d ExternalProvider should be pruned, got err=%v", err)
	}
}
