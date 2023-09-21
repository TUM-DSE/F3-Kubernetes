/*
Copyright 2019 The Kubernetes Authors.

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

package fpgascheduling

import (
	"context"
	"encoding/json"
	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	"k8s.io/kubernetes/pkg/scheduler/internal/cache"
	"strings"
	"testing"
)

var namespaces = []runtime.Object{
	&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "primary"}},
}

func createFPGAStateAnnotation(recentUsage float64, recentReconfigurationTime float64) map[string]string {
	state := FPGANodeState{
		RecentUsage:               recentUsage,
		RecentReconfigurationTime: recentReconfigurationTime,
	}

	serialized, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}

	return map[string]string{
		"fpga-scheduling.io/fpga-state": string(serialized),
	}
}

func TestFPGAScheduling(t *testing.T) {
	tests := []struct {
		pod          *v1.Pod
		pods         []*v1.Pod
		nodes        []*v1.Node
		expectedList framework.NodeScoreList
		name         string

		recentUsageWeight            float64
		recentReconfigurationsWeight float64
		bitstreamLocalityWeight      float64

		wantStatus *framework.Status
	}{
		{
			name: "relative scores",
			pod: &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				EnabledLabel: "true",
			}}},

			recentUsageWeight:            1,
			bitstreamLocalityWeight:      0,
			recentReconfigurationsWeight: 1,

			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Annotations: createFPGAStateAnnotation(0, 0)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Annotations: createFPGAStateAnnotation(5, 5)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Annotations: createFPGAStateAnnotation(10, 10)}},
			},
			expectedList: []framework.NodeScore{{Name: "node1", Score: framework.MaxNodeScore}, {Name: "node2", Score: 66}, {Name: "node3", Score: 33}},
		},
		{
			name: "equal metrics equal scores",
			pod: &v1.Pod{Spec: v1.PodSpec{NodeName: ""}, ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					EnabledLabel: "true",
				},
			}},

			recentUsageWeight:            1,
			bitstreamLocalityWeight:      0,
			recentReconfigurationsWeight: 1,

			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "node1", Annotations: createFPGAStateAnnotation(0, 0)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node2", Annotations: createFPGAStateAnnotation(0, 0)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "node3", Annotations: createFPGAStateAnnotation(0, 0)}},
			},
			expectedList: []framework.NodeScore{{Name: "node1", Score: framework.MaxNodeScore}, {Name: "node2", Score: framework.MaxNodeScore}, {Name: "node3", Score: framework.MaxNodeScore}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := framework.NewCycleState()

			p := plugintesting.SetupPluginWithInformers(ctx, t, New, &config.FPGASchedulingArgs{
				RecentUsageTimeWeight:           test.recentUsageWeight,
				RecentReconfigurationTimeWeight: test.recentReconfigurationsWeight,
				BitstreamLocalityWeight:         test.bitstreamLocalityWeight,
			}, cache.NewSnapshot(test.pods, test.nodes), namespaces)

			status := p.(framework.PreScorePlugin).PreScore(ctx, state, test.pod, test.nodes)

			c, err := state.Read(preScoreStateKey)
			if err != nil {
				t.Errorf("failed to read %q from cycleState: %s", preScoreStateKey, err)
			}

			_, ok := c.(*preScoreState)
			if !ok {
				t.Errorf("%+v  convert to fpgascheduling.preScoreState error", c)
			}

			if !status.IsSuccess() {
				if status.Code() != test.wantStatus.Code() {
					t.Errorf("InterPodAffinity#PreScore() returned unexpected status.Code got: %v, want: %v", status.Code(), test.wantStatus.Code())
				}

				if !strings.Contains(status.Message(), test.wantStatus.Message()) {
					t.Errorf("InterPodAffinity#PreScore() returned unexpected status.Message got: %v, want: %v", status.Message(), test.wantStatus.Message())
				}
				return
			}

			var gotList framework.NodeScoreList
			for _, n := range test.nodes {
				nodeName := n.ObjectMeta.Name
				score, status := p.(framework.ScorePlugin).Score(ctx, state, test.pod, nodeName)
				if !status.IsSuccess() {
					t.Errorf("unexpected error from Score: %v", status)
				}
				gotList = append(gotList, framework.NodeScore{Name: nodeName, Score: score})
			}

			status = p.(framework.ScorePlugin).ScoreExtensions().NormalizeScore(ctx, state, test.pod, gotList)
			if !status.IsSuccess() {
				t.Errorf("unexpected error from NormalizeScore: %v", status)
			}

			if diff := cmp.Diff(test.expectedList, gotList); diff != "" {
				t.Errorf("node score list doesn't match (-want,+got): \n %s", diff)
			}
		})
	}
}
