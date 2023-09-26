package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/kubernetes/scheme"
)

func newTestStore(t *testing.T) *Strategy {
	_ = os.Remove("test.db")
	db, err := gorm.Open(sqlite.Open("memory:test.db"), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			LogLevel: logger.Info,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Table("pod").AutoMigrate(&Record{})
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewStrategy(scheme.Scheme, &corev1.Pod{}, "pod", db, false)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGet(t *testing.T) {
	store := newTestStore(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
		Spec: corev1.PodSpec{
			NodeName: "test",
		},
	}
	_, err := store.Create(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}

	newObj, err := store.Get(context.Background(), "test-namespace", "test-name")
	if err != nil {
		t.Fatal(err)
	}

	newPod := newObj.(*corev1.Pod)
	assert.Equal(t, pod.UID, newPod.UID)
	assert.Equal(t, "test", newPod.Spec.NodeName)
}

func TestCreate(t *testing.T) {
	store := newTestStore(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
	}
	_, err := store.Create(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotEqual(t, types.UID(""), pod.UID)

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
	}
	_, err = store.Create(context.Background(), pod2)
	assert.True(t, apierrors.IsAlreadyExists(err))
}

func TestLabels(t *testing.T) {
	store := newTestStore(t)
	for i := range []*struct{}{nil, nil, nil} {
		d := fmt.Sprint(i)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-name" + d,
				Namespace: "test-namespace",
				Labels: map[string]string{
					"test" + d: d,
					"test":     d,
				},
			},
		}
		_, err := store.Create(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}

	}

	list, err := store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchLabels: map[string]string{
			"test2": "2",
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods := list.(*corev1.PodList)
	assert.Len(t, pods.Items, 1)
	assert.Equal(t, "test-name2", pods.Items[0].Name)

	list, err = store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchLabels: map[string]string{
			"test2": "3",
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods = list.(*corev1.PodList)
	assert.Len(t, pods.Items, 0)

	list, err = store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "test",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"1", "2", "3"},
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods = list.(*corev1.PodList)
	assert.Len(t, pods.Items, 2)
	assert.Equal(t, "test-name1", pods.Items[0].Name)
	assert.Equal(t, "test-name2", pods.Items[1].Name)

	list, err = store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "test",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   []string{"1", "2", "3"},
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods = list.(*corev1.PodList)
	assert.Len(t, pods.Items, 1)
	assert.Equal(t, "test-name0", pods.Items[0].Name)

	list, err = store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "test1",
				Operator: metav1.LabelSelectorOpExists,
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods = list.(*corev1.PodList)
	assert.Len(t, pods.Items, 1)
	assert.Equal(t, "test-name1", pods.Items[0].Name)

	list, err = store.List(context.Background(), "", toLabels(t, metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "test1",
				Operator: metav1.LabelSelectorOpDoesNotExist,
			},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	pods = list.(*corev1.PodList)
	assert.Len(t, pods.Items, 2)
	assert.Equal(t, "test-name0", pods.Items[0].Name)
	assert.Equal(t, "test-name2", pods.Items[1].Name)
	assert.Equal(t, "3", pods.ResourceVersion)
}

func toLabels(t *testing.T, l metav1.LabelSelector) storage.ListOptions {
	res, err := metav1.LabelSelectorAsSelector(&l)
	if err != nil {
		t.Fatal(err)
	}
	return storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Label:    res,
			GetAttrs: storage.DefaultNamespaceScopedAttr,
		},
	}
}

func TestUpdate(t *testing.T) {
	store := newTestStore(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
			Labels: map[string]string{
				"test": "1",
			},
		},
	}
	_, err := store.Create(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}

	newPod := pod.DeepCopy()
	newPod.Spec.NodeName = "hi"
	newPod.Status.Message = "bye"

	newObj, err := store.Update(context.Background(), newPod)
	if err != nil {
		t.Fatal(err)
	}

	newPod = newObj.(*corev1.Pod)
	assert.Equal(t, pod.Generation+1, newPod.Generation)
	assert.Equal(t, "", pod.Status.Message)
	assert.Equal(t, pod.UID, newPod.UID)
}
