#!/usr/bin/env bash
#
# End-to-end harness for spillway.
#
# Brings up a kind cluster (the workload cluster) next to a kcp control plane
# (the offload target), seeds a workspace with a CRD and a few custom resources,
# and then runs the Go assertions in test/e2e.
#
# kcp runs as a container on kind's own docker network. It detects that address
# at startup, writes it into the kubeconfig it generates, and includes it in the
# SANs of its serving certificate -- so the same kubeconfig works unmodified
# from the host and from pods inside the cluster. That is the property spillway
# depends on, so the harness checks it rather than assuming it.
#
# Usage:
#   hack/e2e.sh            # up + test  (leaves the environment running)
#   hack/e2e.sh up         # provision only
#   hack/e2e.sh test       # run the Go tests against a provisioned environment
#   hack/e2e.sh down       # tear everything down
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-spillway-e2e}"
KCP_CONTAINER="${KCP_CONTAINER:-spillway-e2e-kcp}"
KCP_IMAGE="${KCP_IMAGE:-ghcr.io/kcp-dev/kcp:v0.32.3}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.18.0}"
KIND_NETWORK="${KIND_NETWORK:-kind}"
WORKSPACE_NAME="${WORKSPACE_NAME:-spillway}"
E2E_NAMESPACE="${E2E_NAMESPACE:-default}"
API_GROUP="${API_GROUP:-spillway.example.com}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/.e2e}"
GORELEASER="${GORELEASER:-goreleaser}"

KIND_KUBECONFIG="${ARTIFACT_DIR}/kind.kubeconfig"
KCP_KUBECONFIG="${ARTIFACT_DIR}/kcp.kubeconfig"
KCP_WORKSPACE_KUBECONFIG="${ARTIFACT_DIR}/kcp-workspace.kubeconfig"
ENV_FILE="${ARTIFACT_DIR}/e2e.env"

TESTDATA="${REPO_ROOT}/test/e2e/testdata"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m warn\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror\033[0m %s\n' "$*" >&2; exit 1; }

require() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but not on PATH"
}

preflight() {
  require docker
  require kind
  require kubectl
  require go
  require jq
  docker info >/dev/null 2>&1 || die "the docker daemon is not reachable"

  # kcp is reached over docker's bridge network from both the host and the
  # cluster. That routing is a Linux/native-docker property; on Docker Desktop
  # the bridge is inside a VM and unreachable from the host.
  if [[ "$(uname -s)" != "Linux" ]]; then
    warn "this harness assumes native docker on Linux; bridge addresses may not be routable from the host here"
  fi
}

# kcp_address prints the IP kcp is reachable at on the kind network.
kcp_address() {
  docker inspect -f "{{(index .NetworkSettings.Networks \"${KIND_NETWORK}\").IPAddress}}" "${KCP_CONTAINER}"
}

# kkubectl runs kubectl against a kcp workspace URL using the admin credentials.
kkubectl() {
  local url="$1"; shift
  kubectl --kubeconfig "${KCP_KUBECONFIG}" --context root --server "${url}" "$@"
}

up_kind() {
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
    log "kind cluster ${CLUSTER_NAME} already exists"
  else
    log "creating kind cluster ${CLUSTER_NAME}"
    kind create cluster --name "${CLUSTER_NAME}" --kubeconfig "${KIND_KUBECONFIG}"
  fi
  kind export kubeconfig --name "${CLUSTER_NAME}" --kubeconfig "${KIND_KUBECONFIG}" >/dev/null

  # Preload the image used by the in-cluster reachability check so the test does
  # not depend on the cluster being able to pull from a registry.
  log "loading ${CURL_IMAGE} into the cluster"
  docker image inspect "${CURL_IMAGE}" >/dev/null 2>&1 || docker pull "${CURL_IMAGE}" >/dev/null
  kind load docker-image --name "${CLUSTER_NAME}" "${CURL_IMAGE}" >/dev/null
}

up_kcp() {
  if [[ -n "$(docker ps -q --filter "name=^/${KCP_CONTAINER}$")" ]]; then
    log "kcp container ${KCP_CONTAINER} already running"
  else
    docker rm -f "${KCP_CONTAINER}" >/dev/null 2>&1 || true
    log "starting kcp (${KCP_IMAGE}) on the ${KIND_NETWORK} network"
    docker run -d \
      --name "${KCP_CONTAINER}" \
      --network "${KIND_NETWORK}" \
      "${KCP_IMAGE}" start --root-directory=/tmp/kcp >/dev/null
  fi

  log "waiting for kcp to generate its admin kubeconfig"
  local i
  for i in $(seq 1 60); do
    if docker exec "${KCP_CONTAINER}" test -f /tmp/kcp/admin.kubeconfig 2>/dev/null; then
      break
    fi
    if [[ -z "$(docker ps -q --filter "name=^/${KCP_CONTAINER}$")" ]]; then
      docker logs --tail 30 "${KCP_CONTAINER}" >&2 || true
      die "kcp container exited during startup"
    fi
    sleep 1
    [[ "${i}" -eq 60 ]] && die "timed out waiting for kcp to write its kubeconfig"
  done
  docker exec "${KCP_CONTAINER}" cat /tmp/kcp/admin.kubeconfig > "${KCP_KUBECONFIG}"

  local addr
  addr="$(kcp_address)"
  [[ -n "${addr}" ]] || die "kcp container is not attached to the ${KIND_NETWORK} network"
  log "kcp is at https://${addr}:6443"

  log "waiting for kcp to become live"
  for i in $(seq 1 90); do
    if [[ "$(kubectl --kubeconfig "${KCP_KUBECONFIG}" --context root get --raw /livez 2>/dev/null)" == "ok" ]]; then
      break
    fi
    sleep 2
    [[ "${i}" -eq 90 ]] && die "timed out waiting for kcp /livez"
  done
}

seed_kcp() {
  local root_url="https://$(kcp_address):6443/clusters/root"

  log "creating workspace ${WORKSPACE_NAME}"
  kkubectl "${root_url}" apply -f - >/dev/null <<EOF
apiVersion: tenancy.kcp.io/v1alpha1
kind: Workspace
metadata:
  name: ${WORKSPACE_NAME}
spec:
  type:
    name: universal
    path: root
EOF

  local i phase=""
  for i in $(seq 1 60); do
    phase="$(kkubectl "${root_url}" get workspace "${WORKSPACE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ "${phase}" == "Ready" ]] && break
    sleep 1
    [[ "${i}" -eq 60 ]] && die "workspace ${WORKSPACE_NAME} never became Ready (phase=${phase:-<none>})"
  done

  WORKSPACE_URL="$(kkubectl "${root_url}" get workspace "${WORKSPACE_NAME}" -o jsonpath='{.spec.URL}')"
  [[ -n "${WORKSPACE_URL}" ]] || die "workspace ${WORKSPACE_NAME} has no URL"
  log "workspace ready at ${WORKSPACE_URL}"

  log "installing the Widget CRD into the workspace"
  kkubectl "${WORKSPACE_URL}" apply -f "${TESTDATA}/widgets-crd.yaml" >/dev/null
  kkubectl "${WORKSPACE_URL}" wait --for=condition=Established --timeout=120s \
    crd/widgets.spillway.example.com >/dev/null

  log "creating custom resources"
  kkubectl "${WORKSPACE_URL}" create namespace "${E2E_NAMESPACE}" >/dev/null 2>&1 || true
  kkubectl "${WORKSPACE_URL}" apply -n "${E2E_NAMESPACE}" -f "${TESTDATA}/widgets.yaml" >/dev/null
  kkubectl "${WORKSPACE_URL}" get widgets -n "${E2E_NAMESPACE}" >&2

  # Spillway selects its workspace by the server URL of its kubeconfig, so it
  # gets one pointed at the workspace rather than at the shard root.
  cp "${KCP_KUBECONFIG}" "${KCP_WORKSPACE_KUBECONFIG}"
  local cluster
  cluster="$(kubectl --kubeconfig "${KCP_WORKSPACE_KUBECONFIG}" config view \
    -o jsonpath='{.contexts[?(@.name=="root")].context.cluster}')"
  kubectl --kubeconfig "${KCP_WORKSPACE_KUBECONFIG}" config use-context root >/dev/null
  kubectl --kubeconfig "${KCP_WORKSPACE_KUBECONFIG}" config set-cluster "${cluster}" \
    --server="${WORKSPACE_URL}" >/dev/null
}

# build_image produces a local snapshot image through goreleaser -- the single
# build path for this project -- and prints its reference.
build_image() {
  require "${GORELEASER}"

  # goreleaser derives the version from git, so it needs a repository with at
  # least one commit.
  git -C "${REPO_ROOT}" rev-parse HEAD >/dev/null 2>&1 \
    || die "goreleaser needs at least one commit in ${REPO_ROOT} to derive a version; commit first"

  log "building the spillway image with goreleaser" >&2
  # Signing and SBOMs need cosign and syft and are a release-time concern;
  # goreleaser runs those pipes for snapshots too unless told not to.
  "${GORELEASER}" release --snapshot --clean --skip=publish,archive,announce,sbom,sign >&2

  local image
  # goreleaser records the digest tagged image, which is what we want: it is
  # immutable, and never ":latest" -- that would make the kubelet's default pull
  # policy Always for an image that only exists inside the cluster.
  image="$(jq -r '
    map(select(.type == "Docker Manifest" or .type == "Docker Image"))
    | .[0].name // empty' "${REPO_ROOT}/dist/artifacts.json")"
  [[ -n "${image}" ]] || die "goreleaser did not record an image in dist/artifacts.json"

  # It is recorded fully qualified; docker and kind know it by its short form.
  printf '%s' "${image#index.docker.io/library/}"
}

# deploy_spillway builds the image, loads it into the cluster, and registers the
# APIService that hands the group over to spillway.
deploy_spillway() {
  local image
  image="$(build_image)"
  log "built ${image}"

  log "loading the image into the cluster"
  kind load docker-image --name "${CLUSTER_NAME}" "${image}" >/dev/null

  kubectl --kubeconfig "${KIND_KUBECONFIG}" create namespace spillway-system \
    --dry-run=client -o yaml | kubectl --kubeconfig "${KIND_KUBECONFIG}" apply -f - >/dev/null
  kubectl --kubeconfig "${KIND_KUBECONFIG}" -n spillway-system create secret generic spillway-kcp-kubeconfig \
    --from-file=kubeconfig="${KCP_WORKSPACE_KUBECONFIG}" \
    --dry-run=client -o yaml | kubectl --kubeconfig "${KIND_KUBECONFIG}" apply -f - >/dev/null

  log "deploying spillway and registering the APIService"
  sed -e "s|\${SPILLWAY_IMAGE}|${image}|g" -e "s|\${SPILLWAY_GROUP}|${API_GROUP}|g" \
    "${REPO_ROOT}/config/spillway.yaml" \
    | kubectl --kubeconfig "${KIND_KUBECONFIG}" apply -f - >/dev/null

  kubectl --kubeconfig "${KIND_KUBECONFIG}" -n spillway-system \
    rollout status deploy/spillway --timeout=300s >&2

  log "waiting for the APIService to become Available"
  if ! kubectl --kubeconfig "${KIND_KUBECONFIG}" wait --for=condition=Available --timeout=180s \
    "apiservice/v1alpha1.${API_GROUP}" >&2; then
    kubectl --kubeconfig "${KIND_KUBECONFIG}" -n spillway-system logs -l app=spillway --tail=40 >&2 || true
    die "the APIService never became Available"
  fi
}

write_env() {
  cat > "${ENV_FILE}" <<EOF
export KCP_KUBECONFIG='${KCP_KUBECONFIG}'
export KCP_WORKSPACE_URL='${WORKSPACE_URL}'
export KCP_WORKSPACE_KUBECONFIG='${KCP_WORKSPACE_KUBECONFIG}'
export KIND_KUBECONFIG='${KIND_KUBECONFIG}'
export E2E_NAMESPACE='${E2E_NAMESPACE}'
export API_GROUP='${API_GROUP}'
export CURL_IMAGE='${CURL_IMAGE}'
EOF
  log "wrote ${ENV_FILE}"
}

up() {
  preflight
  mkdir -p "${ARTIFACT_DIR}"
  up_kind
  up_kcp
  seed_kcp
  deploy_spillway
  write_env
  log "environment ready -- 'hack/e2e.sh test' to run the assertions, 'hack/e2e.sh down' to clean up"
}

run_tests() {
  [[ -f "${ENV_FILE}" ]] || die "${ENV_FILE} not found; run 'hack/e2e.sh up' first"
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  log "running e2e assertions"
  cd "${REPO_ROOT}"
  go test -tags e2e -count=1 -v -timeout 20m ./test/e2e/...
}

# remove_container deletes a container and waits for it to actually be gone.
# "docker rm -f" can return before removal completes, which would leave the
# container attached to kind's network and block the network from being removed.
remove_container() {
  local name="$1" i
  for i in $(seq 1 30); do
    if [[ -z "$(docker ps -aq --filter "name=^/${name}$")" ]]; then
      return 0
    fi
    docker rm -f "${name}" >/dev/null 2>&1 || true
    sleep 1
  done
  warn "container ${name} is still present; remove it with 'docker rm -f ${name}'"
}

down() {
  # kcp is attached to kind's network, so it has to go first or the network
  # cannot be removed along with the cluster.
  log "removing kcp container"
  remove_container "${KCP_CONTAINER}"

  log "deleting kind cluster ${CLUSTER_NAME}"
  kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
    warn "kind cluster ${CLUSTER_NAME} is still present"
  fi

  rm -rf "${ARTIFACT_DIR}"
  log "done"
}

case "${1:-all}" in
  up)   up ;;
  test) run_tests ;;
  down) down ;;
  all)  up; run_tests ;;
  *)    die "unknown command '${1}'; expected one of: up, test, down, all" ;;
esac
