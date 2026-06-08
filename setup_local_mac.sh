#!/bin/bash
# Run this on your Mac to create a kind cluster and run gen_and_telmetry.py.
# Prerequisites: Docker Desktop (≥14GB RAM), kind, helm, kubectl, Python 3.11/3.12, poetry
set -euo pipefail

AIOPSLAB_DIR="$(cd "$(dirname "$0")" && pwd)"
# MUST stay "kind" — AIOpsLab shell.py hardcodes the Docker container name
# "kind-control-plane" and kubectl.py hardcodes context "kind-kind".
# Any other cluster name silently breaks shell exec and API calls.
CLUSTER_NAME="kind"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# ── Preflight checks ───────────────────────────────────────────────────────────
if ! docker info &>/dev/null; then
    echo "ERROR: Docker Desktop is not running. Start it first, then re-run."
    exit 1
fi

DOCKER_MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
DOCKER_MEM_GB=$((DOCKER_MEM_BYTES / 1073741824))
if [ "$DOCKER_MEM_GB" -lt 12 ]; then
    echo "WARNING: Docker Desktop only has ${DOCKER_MEM_GB} GB RAM allocated."
    echo "  → Docker Desktop → Settings → Resources → Memory → set to 14GB"
    echo "  → Restart Docker Desktop, then re-run this script."
    echo ""
    read -r -p "Continue anyway? [y/N] " reply
    [[ "${reply}" =~ ^[Yy]$ ]] || exit 1
fi

for tool in kind kubectl helm python3; do
    if ! command -v "$tool" &>/dev/null; then
        echo "ERROR: '$tool' not found. Install it first."
        echo "  kind:    brew install kind"
        echo "  kubectl: brew install kubernetes-cli"
        echo "  helm:    brew install helm"
        echo "  python3: brew install python@3.11"
        exit 1
    fi
done

# ── Create or reuse kind cluster ───────────────────────────────────────────────
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    # If the cluster exists but OpenEBS/Prometheus are missing, it's a stale cluster
    # from before Docker was given more memory. Delete and recreate.
    if ! kubectl --context "kind-${CLUSTER_NAME}" get ns openebs &>/dev/null 2>&1 || \
       ! helm --kube-context "kind-${CLUSTER_NAME}" status prometheus -n observe &>/dev/null 2>&1; then
        log "Cluster '${CLUSTER_NAME}' exists but is missing required components — recreating..."
        kind delete cluster --name "$CLUSTER_NAME" || true
    else
        log "Cluster '${CLUSTER_NAME}' already fully configured — reusing."
    fi
fi

if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    log "Creating kind cluster (first run pulls ~2GB image, takes 5-10 min)..."
    cd "$AIOPSLAB_DIR"
    kind create cluster \
        --name "$CLUSTER_NAME" \
        --config kind/kind-config-mac.yaml \
        --wait 10m
fi

kubectl config use-context "kind-${CLUSTER_NAME}"
log "Cluster ready:"
kubectl get nodes -o wide

# ── OpenEBS ────────────────────────────────────────────────────────────────────
if ! kubectl get ns openebs &>/dev/null; then
    log "Installing OpenEBS (storage provisioner)..."
    kubectl apply -f https://openebs.github.io/charts/openebs-operator.yaml
    log "Waiting for OpenEBS pods (up to 10 min for image pull)..."
    kubectl wait pod --all -n openebs \
        --for=condition=Ready \
        --timeout=600s
else
    log "OpenEBS already installed."
fi
kubectl patch storageclass openebs-hostpath \
    -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}' \
    2>/dev/null || true
log "OpenEBS default StorageClass set."

# ── Prometheus ─────────────────────────────────────────────────────────────────
if ! kubectl get ns observe &>/dev/null || ! helm status prometheus -n observe &>/dev/null 2>&1; then
    log "Installing Prometheus..."
    kubectl create namespace observe --dry-run=client -o yaml | kubectl apply -f -
    kubectl apply -f "$AIOPSLAB_DIR/aiopslab/observer/prometheus/prometheus-pvc.yml" -n observe
    helm dependency update "$AIOPSLAB_DIR/aiopslab/observer/prometheus/prometheus/" --quiet
    helm install prometheus "$AIOPSLAB_DIR/aiopslab/observer/prometheus/prometheus/" \
        -n observe --create-namespace
    log "Waiting for Prometheus pods (up to 10 min)..."
    kubectl wait pod --all -n observe \
        --for=condition=Ready \
        --timeout=600s
else
    log "Prometheus already installed."
fi
log "Prometheus ready."

# ── Jaeger ─────────────────────────────────────────────────────────────────────
if ! helm status jaeger -n observability &>/dev/null 2>&1; then
    log "Installing Jaeger (trace collection, optional)..."
    kubectl create namespace observability --dry-run=client -o yaml | kubectl apply -f -
    helm repo add jaegertracing https://jaegertracing.github.io/helm-charts 2>/dev/null || true
    helm repo update 2>/dev/null || true
    helm install jaeger jaegertracing/jaeger \
        --namespace observability \
        --set allInOne.enabled=true \
        --set agent.enabled=false \
        --set collector.enabled=false \
        --set query.enabled=false \
        --set storage.type=memory \
        --timeout 10m0s || log "WARNING: Jaeger install failed — traces unavailable but data gen will continue."
else
    log "Jaeger already installed."
fi

# ── Python env ────────────────────────────────────────────────────────────────
log "Activating Poetry environment..."
cd "$AIOPSLAB_DIR"
eval "$(poetry env activate 2>/dev/null || poetry shell --quiet 2>/dev/null || echo '')"

PYTHON_BIN=""
for py in python3.12 python3.11 python3; do
    if command -v "$py" &>/dev/null; then
        ver=$("$py" -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")
        if [[ "$ver" == "311" || "$ver" == "312" ]]; then
            PYTHON_BIN="$py"
            break
        fi
    fi
done

if [ -z "$PYTHON_BIN" ]; then
    # Try poetry-managed venv
    VENV_PYTHON=$(poetry env info --executable 2>/dev/null || true)
    if [ -n "$VENV_PYTHON" ] && [ -f "$VENV_PYTHON" ]; then
        PYTHON_BIN="$VENV_PYTHON"
    else
        echo "ERROR: Python 3.11 or 3.12 not found. Run: brew install python@3.11"
        exit 1
    fi
fi

log "Using Python: $PYTHON_BIN ($($PYTHON_BIN --version))"

# ── Run data generation ────────────────────────────────────────────────────────
log "Starting gen_and_telmetry.py ..."
"$PYTHON_BIN" "$AIOPSLAB_DIR/gen_and_telmetry.py"

log "Done. Cluster '${CLUSTER_NAME}' is still running."
log "To delete it: kind delete cluster --name ${CLUSTER_NAME}"
