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

/*

Update codegen with hack/update-codegen.sh

How to run this locally (Docker Desktop):

- Run docker build -t registry.k8s.io/kube-scheduler:v1.25.9 -f Dockerfile-scheduler .
- Start integrated Kubernetes cluster in Docker Desktop

To update the image in the cluster:
- Find scheduler container ID with docker container ls and copy it, also copy image ID used by the container
- Run docker build -t registry.k8s.io/kube-scheduler:v1.25.9 -f Dockerfile-scheduler .
- Delete old container and remove image (docker container rm -f <container ID> && docker image rm <image ID>)
- Scheduler will be automatically restarted with the new image

---

How to use this in an existing production cluster:
- Build the image and push it to a registry
- Update the image name provided in the scheduler yaml template in /etc/kubernetes/manifests/kube-scheduler.yaml
- Make sure no other scheduler image is downloaded (docker image ls | grep kube-scheduler)
- Delete the scheduler (kubectl delete pod kube-scheduler -n kube-system)
- Scheduler will be automatically restarted with the new image
- Check that the scheduler is running (kubectl get pods -n kube-system)

How to use this with minikube:
- run eval $(minikube docker-env)
- check out the tag minikube uses for the kube-scheduler image with docker image ls
- Build the image with docker build -t registry.k8s.io/kube-scheduler:<tag> -f Dockerfile-scheduler .
- Delete the kube-scheduler pod (kubectl delete pod kube-scheduler -n kube-system)

---

How to use this with kubeadm:
- Build the image and push it to a registry
- Build other images (kube-apiserver, kube-controller-manager, kube-proxy, kubelet) and push them to a registry
- Set your registry as the preferred image registry in kubeadm: https://kubernetes.io/docs/reference/setup-tools/kubeadm/kubeadm-init/#custom-images
- Running kubeadm init will the images from your custom registry
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

const VendorLabel = "fpga-scheduling.io/fpga-vendor"
const StateLabel = "fpga-scheduling.io/fpga-state"
const EnabledLabel = "fpga-scheduling.io/fpga-enabled"
const BitstreamIdentifierLabel = "fpga-scheduling.io/bitstream-identifier"

func assertVendorLabel(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeInfo *framework.NodeInfo,
) *framework.Status {
	vendorLabel, hasVendorLabel :=
		pod.Labels[""]
	if !hasVendorLabel {
		return nil
	}

	nodeVendorLabel, nodeHasVendorLabel :=
		nodeInfo.Node().Labels[VendorLabel]
	if !nodeHasVendorLabel {
		return nil
	}

	if vendorLabel != nodeVendorLabel {
		return framework.NewStatus(
			framework.UnschedulableAndUnresolvable,
			"Pod requires a different FPGA vendor than the node",
		)
	}

	return nil
}

func (pl *FPGAScheduling) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	status := assertVendorLabel(ctx, state, pod, nodeInfo)
	if status != nil {
		return status
	}

	return framework.NewStatus(framework.Success, "")
}

var _ framework.PreScorePlugin = &FPGAScheduling{}
var _ framework.ScorePlugin = &FPGAScheduling{}
var _ framework.FilterPlugin = &FPGAScheduling{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.FPGAScheduling

const preScoreStateKey = "PreScore" + Name

// Name returns name of the plugin. It is used in logs, etc.
func (pl *FPGAScheduling) Name() string {
	return Name
}

// Subset of struct created in metrics-collector
type FPGANodeState struct {
	Version int

	// exported (and kube-accessible fields)
	RecentUsage                float64             `json:"recentUsage"`
	RecentReconfigurationTime  float64             `json:"recentReconfigurationTime"`
	RecentBitstreamIdentifiers map[string]struct{} `json:"recentBitstreamIdentifiers"`
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

		recentUsage               float64
		recentReconfigurationTime float64
		hasFittingBitstream       bool
	}

	preScores := make([]nodePreScore, 0, len(nodes))
	for _, node := range nodes {
		serializedFPGAState, has := node.Annotations[StateLabel]
		if !has {
			continue
		}

		fpgaState := FPGANodeState{}
		err := json.Unmarshal([]byte(serializedFPGAState), &fpgaState)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to parse %s annotation: %v", StateLabel, err))
		}

		// Don't require every application to set bitstream identifier, so we won't lower the node score if it's not set.
		hasFittingBitstream := true
		bitstreamIdentifierLabel, has := pod.Labels[BitstreamIdentifierLabel]
		if has {
			_, has = fpgaState.RecentBitstreamIdentifiers[bitstreamIdentifierLabel]
			if !has {
				hasFittingBitstream = false
			}
		}

		preScores = append(preScores, nodePreScore{
			nodeName:                  node.Name,
			recentReconfigurationTime: fpgaState.RecentReconfigurationTime,
			recentUsage:               fpgaState.RecentUsage,
			hasFittingBitstream:       hasFittingBitstream,
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
		return sortedByRecentReconfigurations[i].recentReconfigurationTime < sortedByRecentReconfigurations[j].recentReconfigurationTime
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
			if n.recentReconfigurationTime == own.recentReconfigurationTime {
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
		relativeUsageScore, err := getRelativeScore(len(sortedByRecentUsage), recentUsagePosition, pl.args.RecentUsageTimeWeight)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get relative usage score: %v", err))
		}

		recentReconfigurationsPosition, err := getRecentReconfigurationsPosition(sortedByRecentReconfigurations, preScore)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get recent reconfigurations position: %v", err))
		}
		relativeReconfigurationScore, err := getRelativeScore(len(sortedByRecentReconfigurations), recentReconfigurationsPosition, pl.args.RecentReconfigurationTimeWeight)
		if err != nil {
			return framework.NewStatus(framework.Error, fmt.Sprintf("failed to get relative reconfiguration score: %v", err))
		}

		var hasFittingBitstream float64
		if preScore.hasFittingBitstream {
			hasFittingBitstream = pl.args.BitstreamLocalityWeight
		}

		var score = ((relativeReconfigurationScore + relativeUsageScore + hasFittingBitstream) /
			(pl.args.RecentUsageTimeWeight + pl.args.RecentReconfigurationTimeWeight + pl.args.BitstreamLocalityWeight)) * 100

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
	// Skip workloads not interested in FPGA scheduling
	if pod.Labels[EnabledLabel] != "true" {
		return 100, nil
	}

	s, err := getPreScoreState(cycleState)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	score := s.fpgaScore[nodeName]

	fmt.Printf("FPGAScheduling.Score invoked for pod %q on node %q with score %d\n", pod.Name, nodeName, score)

	return score, nil
}

// NormalizeScore normalizes the score for each filteredNode.
func (pl *FPGAScheduling) NormalizeScore(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// No need for normalizing here, scores are already normalized in PreScore stage
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
