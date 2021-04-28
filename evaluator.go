package resource_evaluator

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/validation"
	"strings"
	"time"
)

// the name used for object count quota
var podObjectCountName = ObjectCountQuotaResourceNameFor(v1.SchemeGroupVersion.WithResource("pods").GroupResource())

// PodUsageFunc returns the quota usage for a pod.
// A pod is charged for quota if the following are not true.
//  - pod has a terminal phase (failed or succeeded)
//  - pod has been marked for deletion and grace period has expired
func PodUsageFunc(obj runtime.Object, clock clock.Clock) (v1.ResourceList, error) {
	pod, err := toExternalPodOrError(obj)
	if err != nil {
		return v1.ResourceList{}, err
	}

	// always quota the object count (even if the pod is end of life)
	// object count quotas track all objects that are in storage.
	// where "pods" tracks all pods that have not reached a terminal state,
	// count/pods tracks all pods independent of state.
	result := v1.ResourceList{
		podObjectCountName: *(resource.NewQuantity(1, resource.DecimalSI)),
	}

	// by convention, we do not quota compute resources that have reached end-of life
	// note: the "pods" resource is considered a compute resource since it is tied to life-cycle.
	if !QuotaV1Pod(pod, clock) {
		return result, nil
	}

	requests := v1.ResourceList{}
	limits := v1.ResourceList{}
	// TODO: ideally, we have pod level requests and limits in the future.
	for i := range pod.Spec.Containers {
		requests = Add(requests, pod.Spec.Containers[i].Resources.Requests)
		limits = Add(limits, pod.Spec.Containers[i].Resources.Limits)
	}
	// InitContainers are run sequentially before other containers start, so the highest
	// init container resource is compared against the sum of app containers to determine
	// the effective usage for both requests and limits.
	for i := range pod.Spec.InitContainers {
		requests = Max(requests, pod.Spec.InitContainers[i].Resources.Requests)
		limits = Max(limits, pod.Spec.InitContainers[i].Resources.Limits)
	}

	result = Add(result, podComputeUsageHelper(requests, limits))
	return result, nil
}

func toExternalPodOrError(obj runtime.Object) (*v1.Pod, error) {
	pod := &v1.Pod{}
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	default:
		return nil, fmt.Errorf("expect *api.Pod or *v1.Pod, got %v", t)
	}
	return pod, nil
}

// ObjectCountQuotaResourceNameFor returns the object count quota name for specified groupResource
func ObjectCountQuotaResourceNameFor(groupResource schema.GroupResource) v1.ResourceName {
	if len(groupResource.Group) == 0 {
		return v1.ResourceName("count/" + groupResource.Resource)
	}
	return v1.ResourceName("count/" + groupResource.Resource + "." + groupResource.Group)
}

// QuotaV1Pod returns true if the pod is eligible to track against a quota
// if it's not in a terminal state according to its phase.
func QuotaV1Pod(pod *v1.Pod, clock clock.Clock) bool {
	// if pod is terminal, ignore it for quota
	if v1.PodFailed == pod.Status.Phase || v1.PodSucceeded == pod.Status.Phase {
		return false
	}
	// if pods are stuck terminating (for example, a node is lost), we do not want
	// to charge the user for that pod in quota because it could prevent them from
	// scaling up new pods to service their application.
	if pod.DeletionTimestamp != nil && pod.DeletionGracePeriodSeconds != nil {
		now := clock.Now()
		deletionTime := pod.DeletionTimestamp.Time
		gracePeriod := time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second
		if now.After(deletionTime.Add(gracePeriod)) {
			return false
		}
	}
	return true
}

func Add(a v1.ResourceList, b v1.ResourceList) v1.ResourceList {
	result := v1.ResourceList{}
	for key, value := range a {
		quantity := value.DeepCopy()
		if other, found := b[key]; found {
			quantity.Add(other)
		}
		result[key] = quantity
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

// Max returns the result of Max(a, b) for each named resource
func Max(a v1.ResourceList, b v1.ResourceList) v1.ResourceList {
	result := v1.ResourceList{}
	for key, value := range a {
		if other, found := b[key]; found {
			if value.Cmp(other) <= 0 {
				result[key] = other.DeepCopy()
				continue
			}
		}
		result[key] = value.DeepCopy()
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

// podComputeUsageHelper can summarize the pod compute quota usage based on requests and limits
func podComputeUsageHelper(requests v1.ResourceList, limits v1.ResourceList) v1.ResourceList {
	result := v1.ResourceList{}
	result[v1.ResourcePods] = resource.MustParse("1")
	if request, found := requests[v1.ResourceCPU]; found {
		result[v1.ResourceCPU] = request
		result[v1.ResourceRequestsCPU] = request
	}
	if limit, found := limits[v1.ResourceCPU]; found {
		result[v1.ResourceLimitsCPU] = limit
	}
	if request, found := requests[v1.ResourceMemory]; found {
		result[v1.ResourceMemory] = request
		result[v1.ResourceRequestsMemory] = request
	}
	if limit, found := limits[v1.ResourceMemory]; found {
		result[v1.ResourceLimitsMemory] = limit
	}
	if request, found := requests[v1.ResourceEphemeralStorage]; found {
		result[v1.ResourceEphemeralStorage] = request
		result[v1.ResourceRequestsEphemeralStorage] = request
	}
	if limit, found := limits[v1.ResourceEphemeralStorage]; found {
		result[v1.ResourceLimitsEphemeralStorage] = limit
	}
	for resource, request := range requests {
		// for resources with certain prefix, e.g. hugepages
		if ContainsPrefix(requestedResourcePrefixes, resource) {
			result[resource] = request
			result[maskResourceWithPrefix(resource, v1.DefaultResourceRequestsPrefix)] = request
		}
		// for extended resources
		if IsExtendedResourceName(resource) {
			// only quota objects in format of "requests.resourceName" is allowed for extended resource.
			result[maskResourceWithPrefix(resource, v1.DefaultResourceRequestsPrefix)] = request
		}
	}

	return result
}

// ContainsPrefix returns true if the specified item has a prefix that contained in given prefix Set
func ContainsPrefix(prefixSet []string, item v1.ResourceName) bool {
	for _, prefix := range prefixSet {
		if strings.HasPrefix(string(item), prefix) {
			return true
		}
	}
	return false
}

// maskResourceWithPrefix mask resource with certain prefix
// e.g. hugepages-XXX -> requests.hugepages-XXX
func maskResourceWithPrefix(resource v1.ResourceName, prefix string) v1.ResourceName {
	return v1.ResourceName(fmt.Sprintf("%s%s", prefix, string(resource)))
}

// podResourcePrefixes are the set of prefixes for resources (Hugepages, and other
// potential extended reources with specific prefix) managed by quota associated with pods.
var podResourcePrefixes = []string{
	v1.ResourceHugePagesPrefix,
	v1.ResourceRequestsHugePagesPrefix,
}

// requestedResourcePrefixes are the set of prefixes for resources
// that might be declared in pod's Resources.Requests/Limits
var requestedResourcePrefixes = []string{
	v1.ResourceHugePagesPrefix,
}

// IsExtendedResourceName returns true if:
// 1. the resource name is not in the default namespace;
// 2. resource name does not have "requests." prefix,
// to avoid confusion with the convention in quota
// 3. it satisfies the rules in IsQualifiedName() after converted into quota resource name
func IsExtendedResourceName(name v1.ResourceName) bool {
	if IsNativeResource(name) || strings.HasPrefix(string(name), v1.DefaultResourceRequestsPrefix) {
		return false
	}
	// Ensure it satisfies the rules in IsQualifiedName() after converted into quota resource name
	nameForQuota := fmt.Sprintf("%s%s", v1.DefaultResourceRequestsPrefix, string(name))
	if errs := validation.IsQualifiedName(string(nameForQuota)); len(errs) != 0 {
		return false
	}
	return true
}

// IsNativeResource returns true if the resource name is in the
// *kubernetes.io/ namespace. Partially-qualified (unprefixed) names are
// implicitly in the kubernetes.io/ namespace.
func IsNativeResource(name v1.ResourceName) bool {
	return !strings.Contains(string(name), "/") ||
		IsPrefixedNativeResource(name)
}

// IsPrefixedNativeResource returns true if the resource name is in the
// *kubernetes.io/ namespace.
func IsPrefixedNativeResource(name v1.ResourceName) bool {
	return strings.Contains(string(name), v1.ResourceDefaultNamespacePrefix)
}

// the name used for object count quota
var serviceObjectCountName = ObjectCountQuotaResourceNameFor(v1.SchemeGroupVersion.WithResource("services").GroupResource())

func ServiceUsage(item runtime.Object) (v1.ResourceList, error) {
	result := v1.ResourceList{}
	svc, err := toExternalServiceOrError(item)
	if err != nil {
		return result, err
	}
	ports := len(svc.Spec.Ports)
	// default service usage
	result[serviceObjectCountName] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[v1.ResourceServices] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[v1.ResourceServicesLoadBalancers] = resource.Quantity{Format: resource.DecimalSI}
	result[v1.ResourceServicesNodePorts] = resource.Quantity{Format: resource.DecimalSI}
	switch svc.Spec.Type {
	case v1.ServiceTypeNodePort:
		// node port services need to count node ports
		value := resource.NewQuantity(int64(ports), resource.DecimalSI)
		result[v1.ResourceServicesNodePorts] = *value
	case v1.ServiceTypeLoadBalancer:
		// load balancer services need to count node ports and load balancers
		value := resource.NewQuantity(int64(ports), resource.DecimalSI)
		result[v1.ResourceServicesNodePorts] = *value
		result[v1.ResourceServicesLoadBalancers] = *(resource.NewQuantity(1, resource.DecimalSI))
	}
	return result, nil
}

// convert the input object to an internal service object or error.
func toExternalServiceOrError(obj runtime.Object) (*v1.Service, error) {
	svc := &v1.Service{}
	switch t := obj.(type) {
	case *v1.Service:
		svc = t
	default:
		return nil, fmt.Errorf("expect *api.Service or *v1.Service, got %v", t)
	}
	return svc, nil
}

// the name used for object count quota
var pvcObjectCountName = ObjectCountQuotaResourceNameFor(v1.SchemeGroupVersion.WithResource("persistentvolumeclaims").GroupResource())

const storageClassSuffix string = ".storageclass.storage.k8s.io/"

func PVCUsage(item runtime.Object) (v1.ResourceList, error) {
	result := v1.ResourceList{}
	pvc, err := toExternalPersistentVolumeClaimOrError(item)
	if err != nil {
		return result, err
	}

	// charge for claim
	result[v1.ResourcePersistentVolumeClaims] = *(resource.NewQuantity(1, resource.DecimalSI))
	result[pvcObjectCountName] = *(resource.NewQuantity(1, resource.DecimalSI))
	storageClassRef := GetPersistentVolumeClaimClass(pvc)
	if len(storageClassRef) > 0 {
		storageClassClaim := v1.ResourceName(storageClassRef + storageClassSuffix + string(v1.ResourcePersistentVolumeClaims))
		result[storageClassClaim] = *(resource.NewQuantity(1, resource.DecimalSI))
	}

	// charge for storage
	if request, found := pvc.Spec.Resources.Requests[v1.ResourceStorage]; found {
		result[v1.ResourceRequestsStorage] = request
		// charge usage to the storage class (if present)
		if len(storageClassRef) > 0 {
			storageClassStorage := v1.ResourceName(storageClassRef + storageClassSuffix + string(v1.ResourceRequestsStorage))
			result[storageClassStorage] = request
		}
	}
	return result, nil
}

func toExternalPersistentVolumeClaimOrError(obj runtime.Object) (*v1.PersistentVolumeClaim, error) {
	pvc := &v1.PersistentVolumeClaim{}
	switch t := obj.(type) {
	case *v1.PersistentVolumeClaim:
		pvc = t
	default:
		return nil, fmt.Errorf("expect *api.PersistentVolumeClaim or *v1.PersistentVolumeClaim, got %v", t)
	}
	return pvc, nil
}

// GetPersistentVolumeClaimClass returns StorageClassName. If no storage class was
// requested, it returns "".
func GetPersistentVolumeClaimClass(claim *v1.PersistentVolumeClaim) string {
	// Use beta annotation first
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	return ""
}

// Usage returns the resource usage for the specified object
func ObjectCountUsage(resourceName string) v1.ResourceList {
	quantity := resource.NewQuantity(1, resource.DecimalSI)
	resourceList := v1.ResourceList{}
	resourceList[v1.ResourceName(resourceName)] = *quantity
	return resourceList
}
