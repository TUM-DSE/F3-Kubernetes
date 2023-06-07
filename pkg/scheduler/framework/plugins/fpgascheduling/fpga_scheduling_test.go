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
	"github.com/google/go-cmp/cmp"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	"k8s.io/kubernetes/pkg/scheduler/internal/cache"
	"strings"
	"testing"
)

func TestFPGAScheduling(t *testing.T) {
	tests := []struct {
		pod                                *v1.Pod
		pods                               []*v1.Pod
		nodes                              []*v1.Node
		expectedList                       framework.NodeScoreList
		name                               string
		ignorePreferredTermsOfExistingPods bool
		wantStatus                         *framework.Status
	}{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := framework.NewCycleState()
			p := plugintesting.SetupPluginWithInformers(ctx, t, New, &config.InterPodAffinityArgs{HardPodAffinityWeight: 1, IgnorePreferredTermsOfExistingPods: test.ignorePreferredTermsOfExistingPods}, cache.NewSnapshot(test.pods, test.nodes), namespaces)
			status := p.(framework.PreScorePlugin).PreScore(ctx, state, test.pod, test.nodes)

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
