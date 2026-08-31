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

// Projection of grid-admitted sites into the MaaS catalog.
//
// A site is admitted by the standalone enrollment service (Postgres + gridctl,
// no Kubernetes). Once it joins the grid mesh, the grid operator running in the
// MaaS cluster discovers it over SWIM and materializes a GridSite CR, promoting
// it to Active. This reconciler projects that grid-owned GridSite into the
// MaaS-owned catalog as one ExternalProvider, so the site shows up in the MaaS
// UI. GridSite is the boundary object: enrollment never touches the cluster, the
// grid operator writes GridSite, the maas-controller reads it.
//
// GridSite carries no model list — status.capabilities.inference is a boolean,
// not model names — so the projection stops at the provider (the "site"). Which
// models the site serves is discovered downstream from the provider endpoint;
// this reconciler does not invent ExternalModels it has no source for.
//
// Single-writer boundary: the grid operator owns GridSite; the maas-controller
// owns ExternalProvider. Neither writes the other's kind, so two controllers
// never fight over one resource. The maas-controller depends on the GridSite CR
// contract, never on the grid's Postgres tables.
package maas

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// gridSiteGVK is the grid operator's site CR — the source. Cluster-scoped.
	gridSiteGVK = schema.GroupVersionKind{
		Group: "grid.praxis-proxy.io", Version: "v1alpha1", Kind: "GridSite",
	}
	// externalProviderGVK is the MaaS catalog kind — the projection.
	externalProviderGVK = schema.GroupVersionKind{
		Group: "inference.opendatahub.io", Version: "v1alpha1", Kind: "ExternalProvider",
	}
)

// GridProviderReconciler projects grid GridSite CRs into MaaS ExternalProvider
// resources.
type GridProviderReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// TargetNamespace is where projected ExternalProviders are written. GridSite
	// is cluster-scoped, so the projection cannot inherit a namespace from its
	// source and must be told one — normally the namespace MaaS runs in.
	TargetNamespace string
}

// +kubebuilder:rbac:groups=grid.praxis-proxy.io,resources=gridsites,verbs=get;list;watch
// +kubebuilder:rbac:groups=inference.opendatahub.io,resources=externalproviders,verbs=get;list;watch;create;update;patch;delete

// projectedFromLabel marks a catalog resource as projected from a grid site,
// keyed by the source site name, so stale entries can be found and pruned.
const projectedFromLabel = "maas.opendatahub.io/projected-from-gridsite"

// Reconcile handles create, update and delete for one GridSite.
//
//   - create / update: sync the catalog to what the site projects to. A site
//     that is not Active, or does not offer inference, projects to nothing, so
//     any earlier projection is pruned.
//   - delete: the projected ExternalProvider carries the GridSite as an owner
//     reference, so Kubernetes garbage-collects it; nothing to do here.
func (r *GridProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	site := &unstructured.Unstructured{}
	site.SetGroupVersionKind(gridSiteGVK)
	err := r.Get(ctx, req.NamespacedName, site)
	switch {
	case apierrors.IsNotFound(err):
		return ctrl.Result{}, nil // deleted; owner-reference GC removes the projection
	case err != nil:
		return ctrl.Result{}, err
	case !site.GetDeletionTimestamp().IsZero():
		return ctrl.Result{}, nil // being deleted; GC handles it
	}

	if err := r.sync(ctx, site); err != nil {
		logger.Error(err, "projecting GridSite failed", "gridsite", site.GetName())
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// sync brings the MaaS catalog to exactly what the site projects to: it creates
// or updates the desired ExternalProvider, then prunes any previously-projected
// resource that is no longer desired (e.g. the site left the grid or stopped
// offering inference, so the desired set is empty).
func (r *GridProviderReconciler) sync(ctx context.Context, site *unstructured.Unstructured) error {
	logger := ctrl.LoggerFrom(ctx)

	desired, err := projectExternalResources(site, r.TargetNamespace)
	if err != nil {
		return err
	}

	owner := ownerReference(site)
	wanted := make(map[string]bool, len(desired)) // key: "Kind/name"
	for _, obj := range desired {
		obj.SetOwnerReferences([]metav1.OwnerReference{owner})
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[projectedFromLabel] = site.GetName()
		obj.SetLabels(labels)
		wanted[obj.GetKind()+"/"+obj.GetName()] = true
		if err := r.upsert(ctx, obj); err != nil {
			return fmt.Errorf("upsert %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	pruned, err := r.prune(ctx, site, wanted)
	if err != nil {
		return err
	}
	logger.Info("projected grid site into MaaS catalog",
		"gridsite", site.GetName(), "applied", len(desired), "pruned", pruned)
	return nil
}

// prune deletes catalog resources previously projected from this site that are
// no longer in the desired set.
func (r *GridProviderReconciler) prune(
	ctx context.Context, site *unstructured.Unstructured, wanted map[string]bool,
) (int, error) {
	pruned := 0
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(externalProviderGVK)
	if err := r.List(ctx, list,
		client.InNamespace(r.TargetNamespace),
		client.MatchingLabels{projectedFromLabel: site.GetName()},
	); err != nil {
		return pruned, fmt.Errorf("listing %s to prune: %w", externalProviderGVK.Kind, err)
	}
	for i := range list.Items {
		item := &list.Items[i]
		if wanted[externalProviderGVK.Kind+"/"+item.GetName()] {
			continue
		}
		if err := r.Delete(ctx, item); err != nil && !apierrors.IsNotFound(err) {
			return pruned, fmt.Errorf("pruning %s %s: %w", externalProviderGVK.Kind, item.GetName(), err)
		}
		pruned++
	}
	return pruned, nil
}

// projectExternalResources is the pure projection: a grid GridSite -> one
// ExternalProvider (mTLS, secret <name>-grid-identity). No cluster access, so it
// is fully unit-testable.
//
// A site projects to a provider only when it is Active and offers inference; any
// other state returns an empty set, which prunes an earlier projection. GridSite
// carries no models (capabilities.inference is a boolean), so no ExternalModel is
// emitted — model discovery is a downstream concern off the provider endpoint.
func projectExternalResources(site *unstructured.Unstructured, namespace string) ([]*unstructured.Unstructured, error) {
	name := site.GetName()

	phase, _, _ := unstructured.NestedString(site.Object, "status", "phase")
	inference, _, _ := unstructured.NestedBool(site.Object, "status", "capabilities", "inference")
	if phase != "Active" || !inference {
		return nil, nil // not a projectable inference site; sync() prunes any prior projection
	}

	address, _, _ := unstructured.NestedString(site.Object, "spec", "egress", "address")
	if address == "" {
		return nil, fmt.Errorf("GridSite %s is Active for inference but has no spec.egress.address", name)
	}
	endpoint := address
	if !strings.Contains(address, "://") {
		endpoint = "https://" + address // grid egress is mTLS
	}

	external := &unstructured.Unstructured{}
	external.SetGroupVersionKind(externalProviderGVK)
	external.SetName(name)
	external.SetNamespace(namespace)
	_ = unstructured.SetNestedField(external.Object, name, "spec", "provider")
	_ = unstructured.SetNestedField(external.Object, endpoint, "spec", "endpoint")
	// mTLS: the grid identity MaaS presents when calling the site's egress, not an
	// API key. secretRef by convention; the enrollment identity secret carries the
	// cert. (The upstream ExternalProvider auth shape for mtls is the open ask; the
	// projection emits the intended form.)
	_ = unstructured.SetNestedField(external.Object, "mtls", "spec", "auth", "type")
	_ = unstructured.SetNestedField(external.Object, name+"-grid-identity", "spec", "auth", "secretRef", "name")

	return []*unstructured.Unstructured{external}, nil
}

// ownerReference makes the GridSite own its projected ExternalProvider, so
// deleting the site garbage-collects the catalog entry. Built by hand to avoid
// registering the grid CRD's Go type in the maas-controller scheme.
//
// GridSite is cluster-scoped and the ExternalProvider is namespaced; Kubernetes
// GC supports a cluster-scoped owner of a namespaced dependent. BlockOwnerDeletion
// is left false so the controller needs only read access to the source, not
// update on gridsites/finalizers.
func ownerReference(site *unstructured.Unstructured) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         gridSiteGVK.GroupVersion().String(),
		Kind:               gridSiteGVK.Kind,
		Name:               site.GetName(),
		UID:                site.GetUID(),
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(false),
	}
}

// upsert creates the projected object, or updates it in place if it already exists.
func (r *GridProviderReconciler) upsert(ctx context.Context, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, obj)
	case err != nil:
		return err
	default:
		obj.SetResourceVersion(existing.GetResourceVersion())
		return r.Update(ctx, obj)
	}
}

// SetupWithManager registers the reconciler to watch grid GridSite CRs.
//
// The grid CRD is optional: a MaaS install without the grid has no GridSite
// kind. Watching a kind the API server does not serve would fail the manager's
// informer cache sync and take the whole maas-controller down, so the watch is
// only wired when the CRD is present. If the grid is installed later, the
// maas-controller picks it up on its next restart.
func (r *GridProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if _, err := mgr.GetRESTMapper().RESTMapping(
		gridSiteGVK.GroupKind(), gridSiteGVK.Version,
	); err != nil {
		if meta.IsNoMatchError(err) {
			ctrl.Log.WithName("gridprovider").Info(
				"grid GridSite CRD not installed; skipping grid-to-MaaS projection")
			return nil
		}
		return fmt.Errorf("checking for the GridSite CRD: %w", err)
	}

	source := &unstructured.Unstructured{}
	source.SetGroupVersionKind(gridSiteGVK)
	return ctrl.NewControllerManagedBy(mgr).For(source).Complete(r)
}
