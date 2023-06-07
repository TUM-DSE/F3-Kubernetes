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
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"sort"
)

// FPGAScheduling plugin filters nodes that set node.Spec.Unschedulable=true unless
// the pod tolerates {key=node.kubernetes.io/unschedulable, effect:NoSchedule} taint.
type FPGAScheduling struct {
	args config.FPGASchedulingArgs
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

	type nodePreScore struct {
		nodeName string

		recentUsage            float64
		recentReconfigurations int

		// Don't know yet how to get this info
		hasFittingBitstreamNotImplementedYet bool
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

	sortedByRecentUsage := make([]nodePreScore, len(preScores))
	copy(sortedByRecentUsage, preScores)
	sort.Slice(sortedByRecentUsage, func(i, j int) bool {
		return sortedByRecentUsage[i].recentUsage < sortedByRecentUsage[j].recentUsage
	})

	sortedByRecentReconfigurations := make([]nodePreScore, len(preScores))
	copy(sortedByRecentReconfigurations, preScores)
	sort.Slice(sortedByRecentReconfigurations, func(i, j int) bool {
		return sortedByRecentReconfigurations[i].recentReconfigurations < sortedByRecentReconfigurations[j].recentReconfigurations
	})

	getRecentUsagePosition := func(sorted []nodePreScore, own nodePreScore) (int, error) {
		for i, n := range sorted {
			// with this, equal usage receives equal scores
			// comparing on the name will yield better scores for earlier (in the slice) nodes even if they have the same usage
			if n.recentUsage == own.recentUsage {
				return i, nil
			}
		}
		return -1, fmt.Errorf("node %q not found in sorted list", own.nodeName)
	}

	getRecentReconfigurationsPosition := func(sorted []nodePreScore, own nodePreScore) (int, error) {
		for i, n := range sorted {
			if n.recentReconfigurations == own.recentReconfigurations {
				return i, nil
			}
		}
		return -1, fmt.Errorf("node %q not found in sorted list", own.nodeName)
	}

	getRelativeScore := func(lenSorted int, ownPosition int, weight float64) (float64, error) {
		var relativeScore = (float64(lenSorted - ownPosition)) / float64(lenSorted)
		return relativeScore * weight, nil
	}

	state := &preScoreState{
		fpgaScore: make(map[string]int64),
	}
	for _, preScore := range preScores {
		// perform score calculation
		recentUsagePosition, err := getRecentUsagePosition(sortedByRecentUsage, preScore)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get recent usage position: %v", err))
		}
		relativeUsageScore, err := getRelativeScore(len(sortedByRecentUsage), recentUsagePosition, pl.args.RecentUsageWeight)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get relative usage score: %v", err))
		}

		recentReconfigurationsPosition, err := getRecentReconfigurationsPosition(sortedByRecentReconfigurations, preScore)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get recent reconfigurations position: %v", err))
		}
		relativeReconfigurationScore, err := getRelativeScore(len(sortedByRecentReconfigurations), recentReconfigurationsPosition, pl.args.RecentReconfigurationsWeight)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get relative reconfiguration score: %v", err))
		}

		var hasFittingBitstream float64
		if preScore.hasFittingBitstreamNotImplementedYet {
			hasFittingBitstream = pl.args.HasFittingBitstreamWeight
		}

		var score = float64(
			(relativeReconfigurationScore+relativeUsageScore+hasFittingBitstream)/
				(pl.args.RecentUsageWeight+pl.args.RecentReconfigurationsWeight+pl.args.HasFittingBitstreamWeight)) * 100

		// Limitation: Scheduler expects int64 score
		state.fpgaScore[preScore.nodeName] = int64(score)
	}

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
		return nil, fmt.Errorf("%+v  convert to fpgascheduling.preScoreState error", c)
	}
	return s, nil
}

// Score invoked at the Score extension point.
func (pl *FPGAScheduling) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	s, err := getPreScoreState(cycleState)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	return s.fpgaScore[nodeName], nil
}

// NormalizeScore normalizes the score for each filteredNode.
func (pl *FPGAScheduling) NormalizeScore(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// No need for normalizing here
	// s, err := getPreScoreState(cycleState)
	// if err != nil {
	//	return framework.AsStatus(err)
	// }
	//for i := range scores {
	//	fScore := float64(0)
	//	scores[i].Score = int64(fScore)
	//}

	return nil
}

func getArgs(obj runtime.Object) (config.FPGASchedulingArgs, error) {
	ptr, ok := obj.(*config.FPGASchedulingArgs)
	if !ok {
		return config.FPGASchedulingArgs{}, fmt.Errorf("args are not of type FPGASchedulingArgs, got %T", obj)
	}
	return *ptr, nil
}

// New initializes a new plugin and returns it.
func New(plArgs runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	args, err := getArgs(plArgs)
	if err != nil {
		return nil, err
	}

	return &FPGAScheduling{args}, nil
}

// ScoreExtensions of the Score plugin.
func (pl *FPGAScheduling) ScoreExtensions() framework.ScoreExtensions {
	return pl
}
