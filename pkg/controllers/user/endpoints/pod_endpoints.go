package endpoints

import (
	"strings"

	"fmt"

	workloadutil "github.com/rancher/rancher/pkg/controllers/user/workload"
	"github.com/rancher/types/apis/core/v1"
	"github.com/rancher/types/apis/management.cattle.io/v3"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// This controller is responsible for monitoring pods
// and setting public endpoints on them based on HostPort pods
// and NodePort/LoadBalancer services backing up the pod

type PodsController struct {
	nodeLister         v1.NodeLister
	pods               v1.PodInterface
	podLister          v1.PodLister
	workloadController workloadutil.CommonController
	machinesLister     v3.NodeLister
	clusterName        string
}

func (c *PodsController) sync(key string, obj *corev1.Pod) error {
	if obj == nil && !strings.HasSuffix(key, allEndpoints) {
		return nil
	}

	var pods []*corev1.Pod
	var err error
	if strings.HasSuffix(key, allEndpoints) {
		namespace := ""
		if !strings.EqualFold(key, allEndpoints) {
			namespace = strings.TrimSuffix(key, fmt.Sprintf("/%s", allEndpoints))
		}
		pods, err = c.podLister.List(namespace, labels.NewSelector())
		if err != nil {
			return err
		}
	} else {
		pods = append(pods, obj)
	}

	workloadsToUpdate := map[string]*workloadutil.Workload{}
	nodeNameToMachine, err := getNodeNameToMachine(c.clusterName, c.machinesLister, c.nodeLister)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		updated := c.updatePodEndpoints(pod, nodeNameToMachine)
		if updated {
			workloads, err := c.workloadController.GetWorkloadsMatchingLabels(pod.Namespace, pod.Labels)
			if err != nil {
				return err
			}
			for _, w := range workloads {
				workloadsToUpdate[key] = w
			}
		}
	}

	// push changes to workload
	for _, w := range workloadsToUpdate {
		c.workloadController.EnqueueWorkload(w)
	}

	return nil
}

func podHasHostPort(obj *corev1.Pod) bool {
	for _, c := range obj.Spec.Containers {
		for _, p := range c.Ports {
			if p.HostPort != 0 {
				return true
			}
		}
	}
	return false
}

func (c *PodsController) updatePodEndpoints(obj *corev1.Pod, nodeNameToMachine map[string]*v3.Node) bool {
	if obj.Spec.NodeName == "" {
		return false
	}

	return podHasHostPort(obj
}
