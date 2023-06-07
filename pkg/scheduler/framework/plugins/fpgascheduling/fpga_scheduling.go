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
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"math"
)

// FPGAScheduling plugin filters nodes that set node.Spec.Unschedulable=true unless
// the pod tolerates {key=node.kubernetes.io/unschedulable, effect:NoSchedule} taint.
type FPGAScheduling struct {
	sharedLister framework.SharedLister
}

var _ framework.PreScorePlugin = &FPGAScheduling{}
var _ framework.ScorePlugin = &FPGAScheduling{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.FPGAScheduling

const preScoreStateKey = "PreScore" + Name

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (pl *FPGAScheduling) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add | framework.Update},
	}
}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *FPGAScheduling) Name() string {
	return Name
}

// Subset of struct created in metrics-collector
type FPGANodeState struct {
	RecentUsage            float64 `json:"recentUsage"`
	RecentReconfigurations int     `json:"recentReconfigurations"`
}

type preScoreState struct {
	fpgaScore map[string]int64
}

func (s *preScoreState) Clone() framework.StateData {
	return s
}

// PreScore builds and writes cycle state used by Score and NormalizeScore.
func (pl *FPGAScheduling) PreScore(
	pCtx context.Context,
	cycleState *framework.CycleState,
	pod *v1.Pod,
	nodes []*v1.Node,
) *framework.Status {
	if len(nodes) == 0 {
		// No nodes to score.
		return framework.NewStatus(framework.Skip)
	}

	if pl.sharedLister == nil {
		return framework.NewStatus(framework.Error, "empty shared lister in InterPodAffinity PreScore")
	}

	type nodePreScore struct {
		nodeName string

		recentUsage            float64
		recentReconfigurations int

		// Don't know yet how to get this info
		// hasFittingBitstream bool
	}

	preScores := make([]nodePreScore, 0, len(nodes))
	for _, node := range nodes {
		serializedFPGAState, has := node.Annotations["fpga-state"]
		if !has {
			continue
		}

		fpgaState := FPGANodeState{}
		err := json.Unmarshal([]byte(serializedFPGAState), &fpgaState)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to parse fpga-state annotation: %v", err))
		}

		preScores = append(preScores, nodePreScore{
			nodeName:               node.Name,
			recentReconfigurations: fpgaState.RecentReconfigurations,
			recentUsage:            fpgaState.RecentUsage,
		})
	}

	state := &preScoreState{
		fpgaScore: make(map[string]int64),
	}
	scores := make(map[string]int64)

	cycleState.Write(preScoreStateKey, state)
	return nil
}

func getPreScoreState(cycleState *framework.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to interpodaffinity.preScoreState error", c)
	}
	return s, nil
}

// Score invoked at the Score extension point.
// The "score" returned in this function is the sum of weights got from cycleState which have its topologyKey matching with the node's labels.
// it is normalized later.
// Note: the returned "score" is positive for pod-affinity, and negative for pod-antiaffinity.
func (pl *InterPodAffinity) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := pl.sharedLister.NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("failed to get node %q from Snapshot: %w", nodeName, err))
	}
	node := nodeInfo.Node()

	s, err := getPreScoreState(cycleState)
	if err != nil {
		return 0, framework.AsStatus(err)
	}
	var score int64
	for tpKey, tpValues := range s.topologyScore {
		if v, exist := node.Labels[tpKey]; exist {
			score += tpValues[v]
		}
	}

	return score, nil
}

// NormalizeScore normalizes the score for each filteredNode.
func (pl *InterPodAffinity) NormalizeScore(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	s, err := getPreScoreState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}
	if len(s.topologyScore) == 0 {
		return nil
	}

	var minCount int64 = math.MaxInt64
	var maxCount int64 = math.MinInt64
	for i := range scores {
		score := scores[i].Score
		if score > maxCount {
			maxCount = score
		}
		if score < minCount {
			minCount = score
		}
	}

	maxMinDiff := maxCount - minCount
	for i := range scores {
		fScore := float64(0)
		if maxMinDiff > 0 {
			fScore = float64(framework.MaxNodeScore) * (float64(scores[i].Score-minCount) / float64(maxMinDiff))
		}

		scores[i].Score = int64(fScore)
	}

	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &FPGAScheduling{}, nil
}

// ScoreExtensions of the Score plugin.
func (pl *FPGAScheduling) ScoreExtensions() framework.ScoreExtensions {
	return pl
}
