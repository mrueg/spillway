# Getting started

This walks through offloading one API group to kcp: install a CRD into a kcp workspace, then read
and write those resources with `kubectl` against your **Kubernetes cluster**, and finally look at
the very same objects **directly in kcp**.

At the end, `kubectl get widgets` works in your cluster and `kubectl get crd` shows nothing —
because the resources live in kcp.

## Before you start

You need a Kubernetes cluster you have admin on, `kubectl`, and `openssl`. kcp is deployed into
the cluster as part of the walkthrough, so nothing else is required.

The cluster's aggregation layer must be enabled, which it is on any standard distribution.

```console
export KUBECONFIG=$HOME/.kube/config
```

## 1. Run kcp in the cluster

kcp needs a serving certificate before it starts. It has no `--external-hostname` flag, so the
certificate it would generate itself covers its pod IP and `localhost` — neither of which is the
Service name spillway connects to. One certificate covering both the Service name and `localhost`
lets spillway verify it from inside the cluster and lets you verify it through a port-forward:

```console
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout kcp.key -out kcp.crt -subj "/CN=kcp" \
  -addext "subjectAltName=DNS:kcp,DNS:kcp.kcp.svc,DNS:kcp.kcp.svc.cluster.local,DNS:localhost,IP:127.0.0.1"

kubectl create namespace kcp
kubectl -n kcp create secret tls kcp-serving --cert=kcp.crt --key=kcp.key
```

Then deploy kcp:

```console
kubectl apply -f https://raw.githubusercontent.com/mrueg/spillway/main/docs/kcp.yaml
kubectl -n kcp rollout status deploy/kcp
```

> Storage is an `emptyDir`, so everything in kcp is lost if the pod restarts — including the
> certificate authority it signs client credentials with. If you restart or re-apply the
> Deployment, the kubeconfig you extract below stops authenticating and has to be taken again.
> That is fine for a walkthrough and wrong for anything else.

kcp writes an admin kubeconfig at startup. Take a copy, and point it at kcp through a
port-forward — replacing the certificate authority, since kcp is now serving the certificate you
made rather than one it signed itself:

```console
kubectl -n kcp exec deploy/kcp -- cat /data/admin.kubeconfig > kcp.kubeconfig

kubectl --kubeconfig kcp.kubeconfig config use-context root
kubectl --kubeconfig kcp.kubeconfig config set-cluster root \
  --server=https://localhost:6443/clusters/root \
  --certificate-authority=kcp.crt --embed-certs=true

kubectl -n kcp port-forward svc/kcp 6443:6443 &
kubectl --kubeconfig kcp.kubeconfig get --raw /livez    # ok
```

If the port-forward was already running when the pod was replaced, restart it: it stays attached
to the pod that is gone.

## 2. Create a workspace and install the CRD into it

Spillway serves one workspace, and picks it up from the **server URL** of its kubeconfig rather
than from a separate flag:

```console
kubectl --kubeconfig kcp.kubeconfig apply -f - <<'EOF'
apiVersion: tenancy.kcp.io/v1alpha1
kind: Workspace
metadata:
  name: demo
spec:
  type:
    name: universal
    path: root
EOF

kubectl --kubeconfig kcp.kubeconfig wait --for=jsonpath='{.status.phase}'=Ready workspace/demo

cp kcp.kubeconfig demo.kubeconfig
kubectl --kubeconfig demo.kubeconfig config set-cluster root \
  --server=https://localhost:6443/clusters/root:demo
```

Now install an ordinary CRD. The only thing that makes it special is where it goes:

```console
kubectl --kubeconfig demo.kubeconfig apply -f - <<'EOF'
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.demo.example.com
spec:
  group: demo.example.com
  names:
    kind: Widget
    listKind: WidgetList
    plural: widgets
    singular: widget
    shortNames: [wdg]
  scope: Namespaced
  versions:
    - name: v1alpha1
      served: true
      storage: true
      subresources:
        status: {}
      additionalPrinterColumns:
        - {name: Color, type: string, jsonPath: .spec.color}
        - {name: Size, type: integer, jsonPath: .spec.size}
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              required: [color]
              properties:
                color:
                  type: string
                  enum: [red, green, blue]
                size:
                  type: integer
                  minimum: 1
                  maximum: 100
                  default: 1
            status:
              type: object
              properties:
                phase: {type: string}
EOF

kubectl --kubeconfig demo.kubeconfig wait --for=condition=Established crd/widgets.demo.example.com
```

Widgets are namespaced, so the namespace has to exist **in both places**: in the workspace, which
stores them, and in your cluster, because spillway runs your cluster's namespace admission and will
refuse to create an object in a namespace that cluster does not have.

```console
kubectl --kubeconfig demo.kubeconfig create namespace demo
kubectl create namespace demo
```

## 3. Deploy spillway

Spillway reaches kcp from inside the cluster, by its Service name, so it gets its own copy of the
kubeconfig pointed there:

```console
cp demo.kubeconfig spillway-kcp.kubeconfig
kubectl --kubeconfig spillway-kcp.kubeconfig config set-cluster root \
  --server=https://kcp.kcp.svc:6443/clusters/root:demo

kubectl create namespace spillway-system
kubectl -n spillway-system create secret generic spillway-kcp-kubeconfig \
  --from-file=kubeconfig=spillway-kcp.kubeconfig
```

Then apply the manifests, substituting the image and the group to serve:

```console
curl -sSfL https://raw.githubusercontent.com/mrueg/spillway/main/config/spillway.yaml \
  | sed -e "s|\${SPILLWAY_IMAGE}|ghcr.io/mrueg/spillway:latest|g" \
        -e "s|\${SPILLWAY_GROUP}|demo.example.com|g" \
  | kubectl apply -f -
```

> No image is published until the first release is tagged. Until then, build one locally with
> `make image` and load it into your cluster — with kind that is
> `kind load docker-image <image>` — then use that reference instead.

Wait for it to come up and for the aggregation layer to accept it:

```console
kubectl -n spillway-system rollout status deploy/spillway
kubectl wait --for=condition=Available --timeout=120s apiservice/v1alpha1.demo.example.com
```

`Available=True` means kube-apiserver reached spillway and got a valid discovery response for
`demo.example.com/v1alpha1`.

## 4. Use it through your Kubernetes cluster

Nothing here is spillway-specific — that is the point:

```console
kubectl api-resources --api-group=demo.example.com
```

```
NAME      SHORTNAMES   APIVERSION                      NAMESPACED   KIND
widgets   wdg          demo.example.com/v1alpha1       true         Widget
```

Create one:

```console
kubectl apply -f - <<'EOF'
apiVersion: demo.example.com/v1alpha1
kind: Widget
metadata:
  name: first-widget
  namespace: demo
spec:
  color: red
  size: 7
EOF

kubectl -n demo get widgets
```

The CRD's printer columns, defaulting, and validation all work, because kcp is applying them:

```console
kubectl -n demo get widget first-widget -o jsonpath='{.spec.size}{"\n"}'
kubectl explain widget.spec                          # served from kcp's OpenAPI
kubectl -n demo patch widget first-widget --type=merge -p '{"spec":{"size":1000}}'
# Error ... spec.size: Invalid value: 1000: spec.size in body should be less than or equal to 100
```

Now the part that shows what actually happened:

```console
kubectl get crd | grep widgets     # nothing: no CRD in this cluster
```

Your cluster serves the API. Its etcd stores none of it.

## 5. Look at the same objects directly in kcp

The widget you created through your cluster is a normal object in the workspace:

```console
kubectl --kubeconfig demo.kubeconfig -n demo get widgets
kubectl --kubeconfig demo.kubeconfig -n demo get widget first-widget -o yaml
```

Same object, not a copy — compare the UIDs:

```console
kubectl -n demo get widget first-widget -o jsonpath='{.metadata.uid}{"\n"}'
kubectl --kubeconfig demo.kubeconfig -n demo get widget first-widget -o jsonpath='{.metadata.uid}{"\n"}'
```

It works in the other direction too. Create one in kcp and watch it appear in your cluster:

```console
kubectl --kubeconfig demo.kubeconfig -n demo create -f - <<'EOF'
apiVersion: demo.example.com/v1alpha1
kind: Widget
metadata:
  name: made-in-kcp
spec:
  color: blue
EOF

kubectl -n demo get widgets
```

Adding a *new* CRD to the workspace works the same way — spillway watches the workspace, so it
shows up in your cluster's discovery within seconds, with no restart.

## Two things that will surprise you

**Deleting a namespace does reach kcp.** Your cluster's namespace controller enumerates every
namespaced resource it can discover, spillway's included, so deleting a namespace deletes the
offloaded objects in it — measured at about six seconds for a namespace holding three widgets. The
namespace in the workspace stays; only its contents go.

**A namespace must exist in both places**, unless spillway is run with `--mirror-namespaces`, which
creates the workspace's copy the first time something is written into it. Spillway runs your cluster's namespace admission, so a
widget in a namespace kcp has but your cluster does not is refused with `namespaces "demo" not
found` — even though the widget is stored in kcp.

**An ownerReference cannot point out of the workspace.** Setting one to a ConfigMap or Deployment
in your cluster is refused, because kcp's garbage collector cannot see that owner and would treat
it as deleted, collecting your object within seconds. The error says so. Ownership between objects
in the same workspace works normally.

Your cluster's admission webhooks *do* apply to these resources: spillway runs them before anything
reaches kcp, so a ValidatingWebhookConfiguration covering `demo.example.com` is enforced here just
as it would be for a local CRD.

## When it does not work

`APIService` stuck on `Available=False` is the usual symptom. In order:

```console
# What the aggregation layer thinks is wrong.
kubectl get apiservice v1alpha1.demo.example.com -o jsonpath='{.status.conditions}' | jq

# Whether spillway can see the group at all.
kubectl -n spillway-system logs -l app=spillway --tail=50
```

Spillway reports each configured group separately, which distinguishes "kcp is unreachable" from
"the workspace does not serve that group". Those endpoints skip authorization, like the other
health endpoints, so no credentials are needed — but the image is distroless, so reach them with a
port-forward rather than `kubectl exec`:

```console
kubectl -n spillway-system port-forward deploy/spillway 6443:6443 &

curl -sk https://localhost:6443/healthz/groups                    # all configured groups
curl -sk https://localhost:6443/healthz/groups/demo.example.com   # one, with the reason if it fails
```

`ok` means the workspace serves that group. A failure names it:

```
internal server error: the kcp workspace does not serve group demo.example.com
```

The metrics separate the same two cases, on the same port at `/metrics`. Unlike the health
endpoints they *do* require authorization — a token whose RBAC grants `get` on the `/metrics`
non-resource URL. `spillway_kcp_group_served{group="demo.example.com"}` is `0` when the workspace
genuinely has no such group, while a stale
`spillway_kcp_discovery_last_success_timestamp_seconds` means spillway cannot reach kcp and is
serving its last good answer.

Common causes:

- **The pod cannot reach kcp.** The kubeconfig's server URL has to be routable from pods, and kcp's
  certificate has to cover that address.
- **The workspace does not serve the group.** Check `kubectl --kubeconfig demo.kubeconfig
  api-resources --api-group=demo.example.com`; if it is empty there, spillway has nothing to serve.
- **The CRD is not `Established`.** Spillway serves what discovery reports, and kcp does not report
  a group version until its CRD is established.

## Cleaning up

```console
kubectl delete apiservice v1alpha1.demo.example.com
kubectl delete apiservice -l app.kubernetes.io/managed-by=spillway
kubectl delete namespace spillway-system demo
kubectl delete namespace kcp
```

The second line matters. Spillway registers an APIService for every group version the workspace
serves, and nothing garbage-collects them: an APIService is cluster-scoped, so it cannot be owned by
anything in the namespace you just deleted. Left behind, it points at a Service that no longer
exists, fails the aggregation layer's availability probe, and degrades discovery for the whole
cluster. The label is there so one command finds all of them.

Deleting the kcp namespace takes the workspace and every widget in it with them — they were never
in your cluster's etcd to begin with.
