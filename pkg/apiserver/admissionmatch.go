package apiserver

import (
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	admissionregistrationlisters "k8s.io/client-go/listers/admissionregistration/v1"
	"k8s.io/client-go/tools/cache"
)

// admissionMatcher answers whether anything in the workload cluster would
// actually act on a write to a given resource.
//
// The admission chain cannot answer this. Its Handles method reports whether
// the chain handles an operation at all, and the webhook plugins answer yes
// unconditionally, because they decide per request whether a rule matches.
// Taking that as the answer means spillway resolves every patch against kcp --
// two extra round trips, measured at about 75ms and 40% of patch throughput --
// on clusters where no webhook mentions the resource.
type admissionMatcher struct {
	validating admissionregistrationlisters.ValidatingWebhookConfigurationLister
	mutating   admissionregistrationlisters.MutatingWebhookConfigurationLister
	bindings   admissionregistrationlisters.ValidatingAdmissionPolicyBindingLister

	synced []cache.InformerSynced
}

// matches reports whether a webhook or policy could act on this resource. It is
// deliberately biased towards yes: a wrong no skips admission that should have
// run, so anything unknown -- caches not yet synced, a policy binding whose
// scope this does not attempt to work out -- counts as a match.
func (m *admissionMatcher) matches(gvr schema.GroupVersionResource, operation admission.Operation) bool {
	if m == nil {
		return true
	}

	// Before the caches are populated an empty lister is indistinguishable from
	// a cluster with no webhooks, and getting that wrong means silently
	// skipping policy during startup.
	for _, hasSynced := range m.synced {
		if !hasSynced() {
			return true
		}
	}

	// Admission policies are matched by CEL against constraints this does not
	// interpret. They are rare; the presence of any binding is taken as a
	// match rather than risking a wrong answer.
	if m.bindings != nil {
		if bindings, err := m.bindings.List(labels.Everything()); err != nil || len(bindings) > 0 {
			return true
		}
	}

	wanted := operationType(operation)

	if m.mutating != nil {
		configurations, err := m.mutating.List(labels.Everything())
		if err != nil {
			return true
		}
		for _, configuration := range configurations {
			for _, webhook := range configuration.Webhooks {
				if rulesMatch(webhook.Rules, gvr, wanted) {
					return true
				}
			}
		}
	}

	if m.validating != nil {
		configurations, err := m.validating.List(labels.Everything())
		if err != nil {
			return true
		}
		for _, configuration := range configurations {
			for _, webhook := range configuration.Webhooks {
				if rulesMatch(webhook.Rules, gvr, wanted) {
					return true
				}
			}
		}
	}

	return false
}

func operationType(operation admission.Operation) admissionregistrationv1.OperationType {
	switch operation {
	case admission.Create:
		return admissionregistrationv1.Create
	case admission.Update:
		return admissionregistrationv1.Update
	case admission.Delete:
		return admissionregistrationv1.Delete
	default:
		return admissionregistrationv1.OperationAll
	}
}

func rulesMatch(rules []admissionregistrationv1.RuleWithOperations, gvr schema.GroupVersionResource, operation admissionregistrationv1.OperationType) bool {
	for _, rule := range rules {
		if ruleMatches(rule, gvr, operation) {
			return true
		}
	}
	return false
}

// ruleMatches applies the same wildcards a webhook rule uses: "*" for any group,
// version or resource, and "*/*" or "resource/*" for subresources.
func ruleMatches(rule admissionregistrationv1.RuleWithOperations, gvr schema.GroupVersionResource, operation admissionregistrationv1.OperationType) bool {
	if !containsOperation(rule.Operations, operation) {
		return false
	}
	if !contains(rule.APIGroups, gvr.Group) {
		return false
	}
	if !contains(rule.APIVersions, gvr.Version) {
		return false
	}
	return containsResource(rule.Resources, gvr.Resource)
}

func containsOperation(operations []admissionregistrationv1.OperationType, wanted admissionregistrationv1.OperationType) bool {
	for _, operation := range operations {
		if operation == admissionregistrationv1.OperationAll || operation == wanted {
			return true
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == "*" || value == wanted {
			return true
		}
	}
	return false
}

// containsResource treats "widgets", "*", "widgets/*" and "*/*" as covering the
// resource. A rule naming only a subresource, such as "widgets/status", does not
// cover the resource itself.
func containsResource(resources []string, wanted string) bool {
	for _, resource := range resources {
		switch resource {
		case "*", "*/*", wanted, wanted + "/*":
			return true
		}
	}
	return false
}
