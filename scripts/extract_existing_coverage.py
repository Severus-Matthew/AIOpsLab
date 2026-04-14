import json
from pathlib import Path

ROOT = Path("aiopslab/orchestrator/problems")

TASK_NAMES = ["DetectionTask", "LocalizationTask", "AnalysisTask", "MitigationTask"]
TASK_MAP = {
    "DetectionTask": "detection",
    "LocalizationTask": "localization",
    "AnalysisTask": "analysis",
    "MitigationTask": "mitigation",
}

FAULT_MAP = {
    "k8s_target_port_misconfig": "target_port_misconfig",
    "misconfig_app": "app_misconfig",
    "network_delay": "network_delay",
    "network_loss": "network_loss",
    "pod_failure": "pod_failure",
    "pod_kill": "pod_kill",
    "container_kill": "container_kill",
    "kernel_fault": "kernel_fault",
    "ad_service_high_cpu": "high_cpu",
    "ad_service_failure": "service_failure",
    "ad_service_manual_gc": "manual_gc",
    "cart_service_failure": "service_failure",
    "payment_service_failure": "service_failure",
    "payment_service_unreachable": "service_unreachable",
    "product_catalog_failure": "service_failure",
    "recommendation_service_cache_failure": "cache_failure",
    "kafka_queue_problems": "queue_problem",
    "auth_miss_mongodb": "db_auth_missing",
    "revoke_auth": "db_auth_revoked",
    "redeploy_without_pv": "redeploy_without_pv",
    "assign_non_existent_node": "assign_non_existent_node",
    "scale_pod": "scale_pod",
    "storage_user_unregistered": "storage_user_unregistered",
    "wrong_bin_usage": "wrong_bin_usage",
    "flower_model_misconfig": "model_misconfig",
    "flower_node_stop": "node_stop",
    "operator_misoperation": "operator_misoperation",
    "image_slow_load": "slow_load",
    "loadgenerator_flood_homepage": "load_flood",
    "disk_woreout": "disk_woreout",
    "no_op": "no_op",
}

APP_CLASS_MAP = {
    "HotelReservation": "hotelres",
    "SocialNetwork": "socialnet",
    "AstronomyShop": "astronomy_shop",
    "Flower": "flower",
    "TiDBCluster": "tidb_cluster_operator",
    "FlightTicket": "flight_ticket",
    "TrainTicket": "train_ticket",
}

coverage = []

for d in sorted(ROOT.iterdir()):
    if not d.is_dir():
        continue
    if d.name in {"generated", "__pycache__"}:
        continue

    py_files = list(d.rglob("*.py"))
    if not py_files:
        continue

    text = ""
    for f in py_files:
        try:
            text += f.read_text(encoding="utf-8") + "\n"
        except Exception:
            pass

    apps = sorted({app for cls, app in APP_CLASS_MAP.items() if cls in text})
    tasks = sorted({TASK_MAP[t] for t in TASK_NAMES if t in text})
    fault = FAULT_MAP.get(d.name, d.name)

    for app in apps:
        for task in tasks:
            coverage.append({
                "base_folder": d.name,
                "app": app,
                "task": task,
                "fault_family": fault,
            })

Path("analysis").mkdir(exist_ok=True)
Path("analysis/existing_coverage.json").write_text(json.dumps(coverage, indent=2))
Path("analysis/existing_coverage_tuples.txt").write_text(
    "\n".join(sorted(f"{x['app']},{x['task']},{x['fault_family']}" for x in coverage))
)

print(f"Wrote {len(coverage)} tuples")
