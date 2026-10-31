// SPDX-License-Identifier: AGPL-3.0

package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/go-openapi/testify/v2/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerywatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/netbirdio/kube-egress-forwarder/pkg/forwarder"
)

func TestWatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	group, ctx := errgroup.WithContext(ctx)
	fwd := forwarder.NewForwarder(ctx)
	group.Go(func() error {
		return fwd.Wait()
	})
	kubeClient := fake.NewSimpleClientset()
	group.Go(func() error {
		return Watch(ctx, fwd, kubeClient, "forwarder", "default")
	})

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "forwarder",
			Namespace: "default",
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	require.NoError(t, err)

	cancel()
	err = group.Wait()
	require.NoError(t, err)
}

func TestWatchAppliesRuleChanges(t *testing.T) {
	t.Parallel()

	kubeClient := fake.NewSimpleClientset()
	ctx := startWatch(t, kubeClient)

	first, second := freePort(t), freePort(t)
	createConfigMap(ctx, t, kubeClient, first)
	requireListening(t, first)

	updateConfigMap(ctx, t, kubeClient, second)
	requireListening(t, second)
	requireNotListening(t, first)
}

// The API server closes watches regularly. Rule changes made after that must
// still reach the forwarder.
func TestWatchSurvivesClosedWatch(t *testing.T) {
	t.Parallel()

	kubeClient := fake.NewSimpleClientset()
	var watchCalls atomic.Int32
	kubeClient.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, apimachinerywatch.Interface, error) {
		if watchCalls.Add(1) > 1 {
			return false, nil, nil
		}
		w := apimachinerywatch.NewFake()
		go func() {
			time.Sleep(100 * time.Millisecond)
			w.Stop()
		}()
		return true, w, nil
	})
	ctx := startWatch(t, kubeClient)

	first, second := freePort(t), freePort(t)
	createConfigMap(ctx, t, kubeClient, first)
	requireEventually(t, func() bool { return watchCalls.Load() >= 2 }, "watch was not re-established")
	requireListening(t, first)

	updateConfigMap(ctx, t, kubeClient, second)
	requireListening(t, second)
}

// A watch that fails with "too old resource version" (410) must lead to a new
// list and watch instead of exiting.
func TestWatchSurvivesExpiredResourceVersion(t *testing.T) {
	t.Parallel()

	kubeClient := fake.NewSimpleClientset()
	var listCalls, watchCalls atomic.Int32
	kubeClient.PrependReactor("list", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		listCalls.Add(1)
		return false, nil, nil
	})
	kubeClient.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, apimachinerywatch.Interface, error) {
		if watchCalls.Add(1) > 1 {
			return false, nil, nil
		}
		w := apimachinerywatch.NewFake()
		go w.Error(&metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    410,
			Reason:  metav1.StatusReasonExpired,
			Message: "too old resource version: 1 (150488147)",
		})
		return true, w, nil
	})
	ctx := startWatch(t, kubeClient)

	requireEventually(t, func() bool { return listCalls.Load() >= 2 && watchCalls.Load() >= 2 }, "did not list and watch again after 410")

	port := freePort(t)
	createConfigMap(ctx, t, kubeClient, port)
	requireListening(t, port)
}

func startWatch(t *testing.T, kubeClient *fake.Clientset) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	group, ctx := errgroup.WithContext(ctx)
	fwd := forwarder.NewForwarder(ctx)
	group.Go(func() error {
		return fwd.Wait()
	})
	group.Go(func() error {
		return Watch(ctx, fwd, kubeClient, "forwarder", "default")
	})
	t.Cleanup(func() {
		cancel()
		require.NoError(t, group.Wait())
	})
	return ctx
}

func rulesData(t *testing.T, port int32) map[string]string {
	t.Helper()

	b, err := json.Marshal([]forwarder.Rule{{Protocol: corev1.ProtocolTCP, Port: port, Dest: "127.0.0.1:9"}})
	require.NoError(t, err)
	return map[string]string{forwarder.RuleJsonKey: string(b)}
}

func createConfigMap(ctx context.Context, t *testing.T, kubeClient *fake.Clientset, port int32) {
	t.Helper()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "forwarder", Namespace: "default"},
		Data:       rulesData(t, port),
	}
	_, err := kubeClient.CoreV1().ConfigMaps("default").Create(ctx, cm, metav1.CreateOptions{})
	require.NoError(t, err)
}

func updateConfigMap(ctx context.Context, t *testing.T, kubeClient *fake.Clientset, port int32) {
	t.Helper()

	cm, err := kubeClient.CoreV1().ConfigMaps("default").Get(ctx, "forwarder", metav1.GetOptions{})
	require.NoError(t, err)
	cm.Data = rulesData(t, port)
	_, err = kubeClient.CoreV1().ConfigMaps("default").Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func freePort(t *testing.T) int32 {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return int32(port)
}

func listening(port int32) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func requireListening(t *testing.T, port int32) {
	t.Helper()
	requireEventually(t, func() bool { return listening(port) }, fmt.Sprintf("forwarder is not listening on %d", port))
}

func requireNotListening(t *testing.T, port int32) {
	t.Helper()
	requireEventually(t, func() bool { return !listening(port) }, fmt.Sprintf("forwarder still listens on %d", port))
}

func requireEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}
