package apiserver

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

var widgetsGVR = schema.GroupVersionResource{
	Group: testGroup, Version: "v1alpha1", Resource: "widgets",
}

func rule(groups, versions, resources []string, operations ...admissionregistrationv1.OperationType) admissionregistrationv1.RuleWithOperations {
	return admissionregistrationv1.RuleWithOperations{
		Operations: operations,
		Rule: admissionregistrationv1.Rule{
			APIGroups: groups, APIVersions: versions, Resources: resources,
		},
	}
}

// matcherFor builds a matcher over a synced set of configurations.
func matcherFor(t *testing.T, objects ...runtime.Object) *admissionMatcher {
	t.Helper()

	client := fake.NewSimpleClientset(objects...)
	factory := informers.NewSharedInformerFactory(client, 0)
	group := factory.Admissionregistration().V1()

	matcher := &admissionMatcher{
		validating: group.ValidatingWebhookConfigurations().Lister(),
		mutating:   group.MutatingWebhookConfigurations().Lister(),
		bindings:   group.ValidatingAdmissionPolicyBindings().Lister(),
		synced: []cache.InformerSynced{
			group.ValidatingWebhookConfigurations().Informer().HasSynced,
			group.MutatingWebhookConfigurations().Informer().HasSynced,
			group.ValidatingAdmissionPolicyBindings().Informer().HasSynced,
		},
	}

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	return matcher
}

// A nil matcher is what the callers that have no configuration source use, and
// it must never suppress admission.
func TestNilMatcherAlwaysMatches(t *testing.T) {
	var matcher *admissionMatcher
	if !matcher.matches(widgetsGVR, admission.Update) {
		t.Error("a nil matcher suppressed admission")
	}
}

// An unsynced cache is indistinguishable from a cluster with no webhooks, and
// guessing wrong means skipping policy during startup.
func TestUnsyncedMatcherAlwaysMatches(t *testing.T) {
	matcher := &admissionMatcher{synced: []cache.InformerSynced{func() bool { return false }}}

	if !matcher.matches(widgetsGVR, admission.Update) {
		t.Error("an unsynced matcher suppressed admission")
	}
}

// The case the optimisation exists for.
func TestNoConfigurationsDoesNotMatch(t *testing.T) {
	if matcherFor(t).matches(widgetsGVR, admission.Update) {
		t.Error("a cluster with no webhooks was reported as needing admission")
	}
}

func TestWebhookForTheResourceMatches(t *testing.T) {
	for _, name := range []string{"validating", "mutating"} {
		rules := []admissionregistrationv1.RuleWithOperations{
			rule([]string{testGroup}, []string{"v1alpha1"}, []string{"widgets"}, admissionregistrationv1.Update),
		}

		var matcher *admissionMatcher
		if name == "validating" {
			matcher = matcherFor(t, &admissionregistrationv1.ValidatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{Name: "w"},
				Webhooks:   []admissionregistrationv1.ValidatingWebhook{{Name: "a.b.c", Rules: rules}},
			})
		} else {
			matcher = matcherFor(t, &admissionregistrationv1.MutatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{Name: "w"},
				Webhooks:   []admissionregistrationv1.MutatingWebhook{{Name: "a.b.c", Rules: rules}},
			})
		}

		if !matcher.matches(widgetsGVR, admission.Update) {
			t.Errorf("a %s webhook for this resource was not matched", name)
		}
	}
}

// A webhook for something else must not make every offloaded write pay.
func TestWebhookForAnotherResourceDoesNotMatch(t *testing.T) {
	matcher := matcherFor(t, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "w"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{Name: "a.b.c", Rules: []admissionregistrationv1.RuleWithOperations{
			rule([]string{"apps"}, []string{"v1"}, []string{"deployments"}, admissionregistrationv1.Update),
		}}},
	})

	if matcher.matches(widgetsGVR, admission.Update) {
		t.Error("a webhook for deployments was treated as matching widgets")
	}
}

func TestOperationIsRespected(t *testing.T) {
	matcher := matcherFor(t, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "w"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{Name: "a.b.c", Rules: []admissionregistrationv1.RuleWithOperations{
			rule([]string{testGroup}, []string{"v1alpha1"}, []string{"widgets"}, admissionregistrationv1.Create),
		}}},
	})

	if !matcher.matches(widgetsGVR, admission.Create) {
		t.Error("a CREATE webhook was not matched for a create")
	}
	if matcher.matches(widgetsGVR, admission.Update) {
		t.Error("a CREATE-only webhook was matched for an update")
	}
}

// Rare enough not to be worth interpreting: any binding means admission runs.
func TestAnyPolicyBindingMatches(t *testing.T) {
	matcher := matcherFor(t, &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b"},
	})

	if !matcher.matches(widgetsGVR, admission.Update) {
		t.Error("a policy binding did not force admission")
	}
}

func TestWildcards(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rule  admissionregistrationv1.RuleWithOperations
		match bool
	}{
		{"everything", rule([]string{"*"}, []string{"*"}, []string{"*"}, admissionregistrationv1.OperationAll), true},
		{"group wildcard", rule([]string{"*"}, []string{"v1alpha1"}, []string{"widgets"}, admissionregistrationv1.Update), true},
		{"subresource wildcard", rule([]string{testGroup}, []string{"*"}, []string{"widgets/*"}, admissionregistrationv1.Update), true},
		{"only the subresource", rule([]string{testGroup}, []string{"*"}, []string{"widgets/status"}, admissionregistrationv1.Update), false},
		{"another version", rule([]string{testGroup}, []string{"v1"}, []string{"widgets"}, admissionregistrationv1.Update), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matcher := matcherFor(t, &admissionregistrationv1.ValidatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{Name: "w"},
				Webhooks:   []admissionregistrationv1.ValidatingWebhook{{Name: "a.b.c", Rules: []admissionregistrationv1.RuleWithOperations{tc.rule}}},
			})

			if got := matcher.matches(widgetsGVR, admission.Update); got != tc.match {
				t.Errorf("matches = %v, want %v", got, tc.match)
			}
		})
	}
}
