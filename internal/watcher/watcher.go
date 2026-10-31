// SPDX-License-Identifier: AGPL-3.0

package watcher

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerywatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/netbirdio/kube-egress-forwarder/pkg/forwarder"
)

// Watch keeps the forwarder rules in sync with the ConfigMap until ctx is
// cancelled or a rule update fails. It uses an informer: it lists the ConfigMap
// first, watches from the listed resource version, and lists again whenever
// the API server closes the watch or the resource version has expired.
func Watch(ctx context.Context, fwd *forwarder.Forwarder, kubeClient kubernetes.Interface, cmName, cmNamespace string) error {
	fieldSelector := fmt.Sprintf("metadata.name=%s", cmName)
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return kubeClient.CoreV1().ConfigMaps(cmNamespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (apimachinerywatch.Interface, error) {
			options.FieldSelector = fieldSelector
			return kubeClient.CoreV1().ConfigMaps(cmNamespace).Watch(ctx, options)
		},
	}

	errCh := make(chan error, 1)
	reconcile := func(data map[string]string) {
		ruleMgr, err := forwarder.NewRuleManager(data)
		if err == nil {
			err = fwd.Reconcile(ruleMgr.AllRules())
		}
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}
	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: cache.ToListWatcherWithWatchListSemantics(lw, kubeClient),
		ObjectType:    &corev1.ConfigMap{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				if cm, ok := obj.(*corev1.ConfigMap); ok {
					reconcile(cm.Data)
				}
			},
			UpdateFunc: func(_, obj any) {
				if cm, ok := obj.(*corev1.ConfigMap); ok {
					reconcile(cm.Data)
				}
			},
			DeleteFunc: func(any) {
				reconcile(nil)
			},
		},
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go controller.RunWithContext(ctx)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}
