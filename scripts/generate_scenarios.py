import json
import itertools
import random
from pathlib import Path
from collections import defaultdict

APPS = [
    "hotelres",
    "socialnet",
    "astronomy_shop",
    "flower",
    "tidb_cluster_operator",
]

TASKS = ["detection", "localization", "analysis", "mitigation"]

WORKLOADS = [
    {"pattern": "steady_low", "rate": 50},
    {"pattern": "steady_mid", "rate": 100},
    {"pattern": "steady_high", "rate": 150},
    {"pattern": "burst_low", "rate": 100},
    {"pattern": "burst_high", "rate": 250},
    {"pattern": "diurnal", "rate": 120},
]

SEVERITIES = ["low", "medium", "high", "critical"]

TARGETS_BY_APP = {
    "hotelres": ["geo", "profile", "rate", "recommendation", "reservation", "search", "user", "db"],
    "socialnet": ["user-service", "text-service", "post-storage-service", "compose-post-service", "db"],
    "astronomy_shop": [
        "adservice",
        "cartservice",
        "checkoutservice",
        "currencyservice",
        "emailservice",
        "paymentservice",
        "productcatalogservice",
        "recommendationservice",
        "shippingservice",
        "frontend",
        "cache",
    ],
    "flower": ["frontend", "worker"],
    "tidb_cluster_operator": ["operator", "scheduler", "db"],
}

def load_existing():
    path = Path("analysis/existing_coverage.json")
    if not path.exists():
        return []
    return json.loads(path.read_text())

EXISTING = load_existing()

# Build allowed families from repo coverage.
ALLOWED_BY_APP_TASK = defaultdict(set)
for row in EXISTING:
    ALLOWED_BY_APP_TASK[(row["app"], row["task"])].add(row["fault_family"])

def valid(app, task, fault, workload, severity, target):
    # Only allow families the repo already supports for that app/task.
    if fault not in ALLOWED_BY_APP_TASK.get((app, task), set()):
        return False

    if target not in TARGETS_BY_APP[app]:
        return False

    # App/fault/target sanity rules.
    if app == "hotelres" and fault in {"db_auth_revoked", "storage_user_unregistered"} and target not in {"geo", "rate", "db"}:
        return False
    if app == "socialnet" and fault == "target_port_misconfig" and target not in {"user-service", "text-service", "post-storage-service"}:
        return False
    if app == "socialnet" and fault == "db_auth_missing" and target != "db":
        return False
    if app == "socialnet" and fault == "assign_non_existent_node" and target not in {"compose-post-service", "user-service", "text-service"}:
        return False
    if app == "astronomy_shop" and fault == "cache_failure" and target not in {"cache", "recommendationservice", "frontend"}:
        return False
    if app == "astronomy_shop" and fault == "high_cpu" and target != "adservice":
        return False
    if app == "astronomy_shop" and fault == "manual_gc" and target != "adservice":
        return False
    if app == "astronomy_shop" and fault == "service_unreachable" and target != "paymentservice":
        return False
    if app == "astronomy_shop" and fault == "slow_load" and target != "frontend":
        return False
    if app == "astronomy_shop" and fault == "load_flood" and target != "frontend":
        return False
    if app == "flower" and target not in {"frontend", "worker"}:
        return False
    if app == "tidb_cluster_operator" and target not in {"operator", "scheduler", "db"}:
        return False

    return True

def scenario_id(app, task, fault, workload, severity, target, idx):
    return f"{app}_{task}_{fault}_{workload['pattern']}_{severity}_{target}_{idx:05d}"

def make_scenario(app, task, fault, workload, severity, target, idx):
    return {
        "scenario_id": scenario_id(app, task, fault, workload, severity, target, idx),
        "app": app,
        "task": task,
        "fault": {
            "type": fault,
            "target": target,
            "severity": severity,
            "start_delay_sec": 60,
            "duration_sec": 180,
        },
        "workload": {
            "type": "wrk",
            "pattern": workload["pattern"],
            "rate": workload["rate"],
            "duration_sec": 300,
        },
        "telemetry": {
            "logs": True,
            "metrics": True,
            "traces": True,
        },
        "evaluation": {
            "max_steps": 10 if task != "mitigation" else 15,
        },
    }

def generate_all():
    scenarios = []
    idx = 0
    for app in APPS:
        for task in TASKS:
            allowed_faults = sorted(ALLOWED_BY_APP_TASK.get((app, task), set()))
            for fault in allowed_faults:
                for workload in WORKLOADS:
                    for severity in SEVERITIES:
                        for target in TARGETS_BY_APP[app]:
                            if valid(app, task, fault, workload, severity, target):
                                idx += 1
                                scenarios.append(
                                    make_scenario(app, task, fault, workload, severity, target, idx)
                                )
    return scenarios

def write_split(scenarios, limit=7000):
    random.seed(42)
    random.shuffle(scenarios)
    scenarios = scenarios[:limit]

    splits = {
        "train": scenarios[:4000],
        "val": scenarios[4900:5600],
        "test": scenarios[5600:7000],
    }

    for split, items in splits.items():
        outdir = Path("scenario_specs") / split
        outdir.mkdir(parents=True, exist_ok=True)
        for s in items:
            (outdir / f"{s['scenario_id']}.json").write_text(json.dumps(s, indent=2))

    print(f"Wrote {len(scenarios)} scenario specs")

if __name__ == "__main__":
    pool = generate_all()
    Path("analysis/generated_pool_count.txt").write_text(str(len(pool)))
    print(f"Total generated valid scenarios: {len(pool)}")
    write_split(pool, limit=7000)
