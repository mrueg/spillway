package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// A CRD list as the workspace returns it: one group spillway serves, one it
// does not.
const crdList = `{
  "apiVersion": "apiextensions.k8s.io/v1",
  "kind": "CustomResourceDefinitionList",
  "items": [
    {
      "metadata": {"name": "widgets.spillway.example.com"},
      "spec": {
        "group": "spillway.example.com",
        "names": {"kind": "Widget", "listKind": "WidgetList", "plural": "widgets", "singular": "widget"},
        "scope": "Namespaced",
        "versions": [
          {"name": "v1alpha1", "served": true, "storage": true,
           "schema": {"openAPIV3Schema": {"type": "object", "properties": {
              "spec": {"type": "object", "properties": {"color": {"type": "string"}}}}}}},
          {"name": "v1beta1", "served": false, "storage": false,
           "schema": {"openAPIV3Schema": {"type": "object"}}}
        ]
      }
    },
    {
      "metadata": {"name": "workspaces.tenancy.kcp.io"},
      "spec": {
        "group": "tenancy.kcp.io",
        "names": {"kind": "Workspace", "listKind": "WorkspaceList", "plural": "workspaces", "singular": "workspace"},
        "scope": "Cluster",
        "versions": [{"name": "v1alpha1", "served": true, "storage": true,
          "schema": {"openAPIV3Schema": {"type": "object"}}}]
      }
    }
  ]
}`

// decodeForTest turns the fixture into what buildV2 now takes.
func decodeForTest(t *testing.T, list string) []apiextensionsv1.CustomResourceDefinition {
	t.Helper()

	definitions, err := decodeCRDList([]byte(list))
	if err != nil {
		t.Fatalf("decoding the CRD list fixture: %v", err)
	}
	return definitions
}

func TestV2DescribesTheOwnedGroup(t *testing.T) {
	document, err := buildV2(decodeForTest(t, crdList), sets.New("spillway.example.com"))
	if err != nil {
		t.Fatalf("buildV2: %v", err)
	}

	var described []string
	for path := range document.Paths.Paths {
		described = append(described, path)
	}
	if len(described) == 0 {
		t.Fatal("no paths were described at all")
	}

	var sawWidgets bool
	for _, path := range described {
		if path == "/apis/spillway.example.com/v1alpha1/namespaces/{namespace}/widgets" {
			sawWidgets = true
		}
	}
	if !sawWidgets {
		t.Errorf("the namespaced widgets path is missing: %v", described)
	}

	var sawWidgetDefinition bool
	for name := range document.Definitions {
		if name == "com.example.spillway.v1alpha1.Widget" {
			sawWidgetDefinition = true
		}
	}
	if !sawWidgetDefinition {
		t.Errorf("the Widget schema is missing from the definitions")
	}
}

// A definition merged into the cluster's spec for a type the cluster does not
// serve is worse than no document at all.
func TestV2IgnoresGroupsSpillwayDoesNotServe(t *testing.T) {
	document, err := buildV2(decodeForTest(t, crdList), sets.New("spillway.example.com"))
	if err != nil {
		t.Fatalf("buildV2: %v", err)
	}

	for path := range document.Paths.Paths {
		if strings.Contains(path, "tenancy.kcp.io") {
			t.Errorf("%s describes a group spillway does not serve", path)
		}
	}
	for name := range document.Definitions {
		if strings.Contains(name, "Workspace") {
			t.Errorf("%s is a definition for a group spillway does not serve", name)
		}
	}
}

// A version that is not served has no endpoints, so describing it would
// advertise something that answers 404.
func TestV2SkipsUnservedVersions(t *testing.T) {
	document, err := buildV2(decodeForTest(t, crdList), sets.New("spillway.example.com"))
	if err != nil {
		t.Fatalf("buildV2: %v", err)
	}

	for path := range document.Paths.Paths {
		if strings.Contains(path, "v1beta1") {
			t.Errorf("%s describes a version the workspace does not serve", path)
		}
	}
}

func TestV2WithNothingOwned(t *testing.T) {
	document, err := buildV2(decodeForTest(t, crdList), sets.New("nothing.example.com"))
	if err != nil {
		t.Fatalf("buildV2: %v", err)
	}

	if len(document.Paths.Paths) != 0 {
		t.Errorf("got %d paths, want none", len(document.Paths.Paths))
	}
	if document.Swagger != "2.0" {
		t.Errorf("swagger = %q, want 2.0 even when empty", document.Swagger)
	}
	if document.Info == nil {
		t.Error("info is missing, which makes the document invalid")
	}
}

func TestV2RejectsAnUnparseableList(t *testing.T) {
	if _, err := decodeCRDList([]byte("not json")); err == nil {
		t.Error("an unparseable CRD list was accepted")
	}
}

func TestServeV2ReportsAnUnreachableKCP(t *testing.T) {
	h := newOpenAPIHandler(&fakeFetcher{docs: map[string][]byte{}})

	if recorder := get(t, h.serveV2, "/openapi/v2"); recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", recorder.Code)
	}
}

func TestServeV2ServesWhatItBuilt(t *testing.T) {
	h := newOpenAPIHandler(&fakeFetcher{docs: map[string][]byte{crdPath: []byte(crdList)}})

	recorder := get(t, h.serveV2, "/openapi/v2")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
	}

	var swagger map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &swagger); err != nil {
		t.Fatalf("the served document is not valid JSON: %v", err)
	}
	if swagger["swagger"] != "2.0" {
		t.Errorf("swagger = %v, want 2.0", swagger["swagger"])
	}
	paths, _ := swagger["paths"].(map[string]any)
	if len(paths) == 0 {
		t.Error("the served document describes nothing")
	}
}

// failingFetcher stands in for a workspace that cannot answer for its
// CustomResourceDefinitions.
type failingFetcher struct{ err error }

func (f failingFetcher) fetchSpec(context.Context, string) ([]byte, error) { return nil, f.err }

func v2Handler(sources ...namedFetcher) *openAPIHandler {
	h := &openAPIHandler{
		cache:    fakeSnapshotter{snapshot: widgetSnapshot()},
		groups:   func() sets.Set[string] { return sets.New(testGroup) },
		fetchers: func() []namedFetcher { return sources },
	}
	h.prepare()
	return h
}

// There is one v2 document for the whole server, so a workspace that cannot
// answer must not take kubectl explain away from every group spillway serves.
// An APIExport's virtual endpoint never answers this, and that is a normal
// configuration rather than an outage.
func TestV2SurvivesAWorkspaceThatCannotAnswer(t *testing.T) {
	handler := v2Handler(
		namedFetcher{name: "workspace", fetcher: &fakeFetcher{docs: map[string][]byte{crdPath: []byte(crdList)}}},
		namedFetcher{name: "virtual", fetcher: failingFetcher{
			err: &backendStatusError{status: http.StatusNotFound, path: crdPath}}},
	)

	encoded, err := handler.v2.get(context.Background(), 1)
	if err != nil {
		t.Fatalf("building the document with one source unavailable: %v", err)
	}
	if !strings.Contains(string(encoded), "Widget") {
		t.Errorf("the document does not describe the workspace that did answer: %s", encoded)
	}
}

// If nothing can be asked, the document is not built: an empty one merged into
// the cluster's spec says these groups have no schemas, which is worse than the
// last good answer.
func TestV2RefusesWhenNoWorkspaceCanAnswer(t *testing.T) {
	handler := v2Handler(
		namedFetcher{name: "one", fetcher: failingFetcher{err: errors.New("unreachable")}},
		namedFetcher{name: "two", fetcher: failingFetcher{err: errors.New("unreachable")}},
	)

	if _, err := handler.v2.get(context.Background(), 1); err == nil {
		t.Error("a document was built although no workspace could be asked")
	}
}
