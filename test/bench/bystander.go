//go:build bench

package bench

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// bystander measures what the workload does to traffic that has nothing to do
// with it.
//
// This is the argument for offloading, and until now nothing here tested it.
// Everything else on this page says spillway is slower, which it is and must
// be. What it is supposed to buy is that the cluster's own apiserver and etcd
// stop carrying the objects -- and if that is worth anything, then somebody
// writing an unrelated ConfigMap while three thousand custom resources are
// created should notice the difference between those resources being native and
// being offloaded.
//
// A write rather than a read, because a read of an unchanging object is served
// from the watch cache and would measure almost nothing. Creating and deleting a
// small ConfigMap goes to etcd, which is the thing offloading is meant to
// protect.
type bystander struct {
	client kubernetes.Interface
	// every is the interval between samples. Slow enough that the measurement
	// is not itself a load test.
	every time.Duration
}

// watch samples until the returned function is called, which stops it and
// returns what it saw.
func (b *bystander) watch(ctx context.Context, label string) func() *sample {
	measured := &sample{}
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(done)

		ticker := time.NewTicker(b.every)
		defer ticker.Stop()

		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			name := fmt.Sprintf("bystander-%s-%d", label, i)
			started := time.Now()
			err := b.write(ctx, name)
			measured.record(time.Since(started), err)
		}
	}()

	return func() *sample {
		cancel()
		<-done
		return measured
	}
}

// write is one unrelated round trip to the cluster's own storage.
func (b *bystander) write(ctx context.Context, name string) error {
	if _, err := b.client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data:       map[string]string{"measured": "traffic that has nothing to do with the workload"},
	}, metav1.CreateOptions{}); err != nil {
		return err
	}
	return b.client.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}
