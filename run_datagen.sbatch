#!/bin/bash
#SBATCH --job-name=aiopslab-datagen
#SBATCH --account=bgrm-dtai-gh
#SBATCH --partition=ghx4
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=96G
#SBATCH --time=36:00:00
#SBATCH --array=0-3                  # 4 parallel workers → ~33 h instead of 130 h
#SBATCH --output=/u/mjha1/IBM_PROJECT/multiagent_IBM/logs/datagen_%A_%a.out
#SBATCH --error=/u/mjha1/IBM_PROJECT/multiagent_IBM/logs/datagen_%A_%a.err

set -euo pipefail

# ── Paths & env ────────────────────────────────────────────────────────────────
export PATH="$HOME/.local/bin:$PATH"
export KIND_EXPERIMENTAL_PROVIDER=podman

# Podman rootless on HPC compute nodes fails because:
#   1. SLURM inherits XDG_RUNTIME_DIR=/run/user/$UID from the login session
#   2. That path doesn't exist on compute nodes (no systemd-logind session)
#   3. Podman queries D-Bus for the runtime dir, overriding our env var
#   4. Podman recalculates runRoot/graphRoot from XDG vars, ignoring storage.conf
# Fix: redirect ALL XDG dirs, cut D-Bus, and force file-based events.
PODMAN_TMP="${TMPDIR:-/tmp}/podman-${SLURM_JOB_ID:-$$}"
mkdir -p "${PODMAN_TMP}/run" "${PODMAN_TMP}/storage" "${PODMAN_TMP}/data"
chmod 0700 "${PODMAN_TMP}"

export XDG_RUNTIME_DIR="${PODMAN_TMP}/run"   # → runRoot
export XDG_DATA_HOME="${PODMAN_TMP}/data"     # → graphRoot
unset  DBUS_SESSION_BUS_ADDRESS               # prevent D-Bus from overriding

cat > "${PODMAN_TMP}/storage.conf" << EOF
[storage]
driver = "overlay"
runroot = "${PODMAN_TMP}/run"
graphroot = "${PODMAN_TMP}/storage"
[storage.options.overlay]
ignore_chown_errors = "true"
EOF
export CONTAINERS_STORAGE_CONF="${PODMAN_TMP}/storage.conf"

# events_logger=file avoids creating dirs under XDG_RUNTIME_DIR at startup
cat > "${PODMAN_TMP}/containers.conf" << 'PODMAN_CONF'
[engine]
cgroup_manager = "cgroupfs"
events_logger = "file"
PODMAN_CONF
export CONTAINERS_CONF="${PODMAN_TMP}/containers.conf"

AIOPSLAB_DIR="/u/mjha1/IBM_PROJECT/multiagent_IBM/AIOpsLab"
CLUSTER_NAME="aiopslab-datagen"

# Parallel sharding: each SLURM array task runs on its own compute node with its
# own kind cluster, and processes 1/N of the 1583 scenarios.
WORKER_ID="${SLURM_ARRAY_TASK_ID:-0}"
NUM_WORKERS="${SLURM_ARRAY_TASK_COUNT:-1}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] worker=${WORKER_ID}/${NUM_WORKERS} $*"; }

log "Running on node: $(hostname), arch: $(uname -m)"
log "CPUs: $(nproc), Memory: $(free -h | awk '/^Mem/{print $2}')"

# ── Cleanup on exit ─────────────────────────────────────────────────────────────
cleanup() {
    log "Cleaning up kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# ── Prepare /run/udev for OpenEBS NDM ──────────────────────────────────────────
# OpenEBS Node Disk Manager mounts /run/udev; must be a directory.
if [ ! -d /run/udev ]; then
    mkdir -p /run/udev 2>/dev/null || true
fi

# ── Create kind cluster (x86_64 on Delta AI) ────────────────────────────────────
log "Creating kind cluster (this pulls the node image on first run ~5-10 min)..."
cd "$AIOPSLAB_DIR"
kind create cluster \
    --name "$CLUSTER_NAME" \
    --config kind/kind-config-x86.yaml \
    --wait 10m

kubectl config use-context "kind-${CLUSTER_NAME}"
log "Cluster ready:"
kubectl get nodes -o wide

# ── Install OpenEBS (storage provisioner needed by Helm PVCs) ──────────────────
log "Installing OpenEBS..."
kubectl apply -f https://openebs.github.io/charts/openebs-operator.yaml
log "Waiting for OpenEBS pods (up to 10 min for cold image pull)..."
kubectl wait pod --all -n openebs \
    --for=condition=Ready \
    --timeout=600s
kubectl patch storageclass openebs-hostpath \
    -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
log "OpenEBS ready."

# ── Install Prometheus (bundled chart in repo) ──────────────────────────────────
log "Installing Prometheus..."
kubectl create namespace observe --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f aiopslab/observer/prometheus/prometheus-pvc.yml -n observe
helm dependency update aiopslab/observer/prometheus/prometheus/
helm install prometheus aiopslab/observer/prometheus/prometheus/ \
    -n observe --create-namespace
log "Waiting for Prometheus pods (up to 10 min for cold image pull)..."
kubectl wait pod --all -n observe \
    --for=condition=Ready \
    --timeout=600s
log "Prometheus ready."

# ── Install Jaeger (all-in-one, for trace collection) ──────────────────────────
log "Installing Jaeger all-in-one..."
kubectl create namespace observability --dry-run=client -o yaml | kubectl apply -f -
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts 2>/dev/null || true
helm repo update --fail-on-repo-update-fail 2>/dev/null || true
helm install jaeger jaegertracing/jaeger \
    --namespace observability \
    --set allInOne.enabled=true \
    --set agent.enabled=false \
    --set collector.enabled=false \
    --set query.enabled=false \
    --set storage.type=memory \
    --timeout 10m0s || log "WARNING: Jaeger install failed — trace collection may be unavailable."
log "Jaeger install complete (traces optional, will not block data gen)."

# ── Activate Poetry Python env ──────────────────────────────────────────────────
log "Activating Python environment..."
eval $(poetry env activate)
python3.11 --version

# ── Run data generation ─────────────────────────────────────────────────────────
log "Starting gen_and_telmetry.py (worker ${WORKER_ID}/${NUM_WORKERS})..."
AIOPSLAB_WORKER_ID="${WORKER_ID}" \
AIOPSLAB_NUM_WORKERS="${NUM_WORKERS}" \
AIOPSLAB_OUT_DIR="/u/mjha1/IBM_PROJECT/multiagent_IBM/AIOpsLab/dynamic_generated_scenario_results_all_new" \
AIOPSLAB_CLUSTER="${CLUSTER_NAME}" \
python3.11 gen_and_telmetry.py

log "Data generation complete."
