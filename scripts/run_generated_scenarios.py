import asyncio
import json
import time
from pathlib import Path

from aiopslab.orchestrator import Orchestrator

OUT_ROOT = Path("dataset/raw")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

def save_text(path: Path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content))

def load_problem_ids(limit=None):
    ids = [p.stem for p in Path("scenario_specs").rglob("*.json")]
    ids = sorted(
        x for x in ids
        if x.startswith("hotelres_") and "_app_misconfig_" in x
    )
    return ids if limit is None else ids[:limit]

async def snapshot_api(session_apis, scenario_dir: Path, namespace: str, service: str, stage: str):
    try:
        out = session_apis["exec_shell"](f"kubectl get pods -n {namespace} -o wide")
        save_text(scenario_dir / f"{stage}_pods.txt", out)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_pods_error.txt", e)

    try:
        out = session_apis["exec_shell"](f"kubectl get svc -n {namespace}")
        save_text(scenario_dir / f"{stage}_svc.txt", out)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_svc_error.txt", e)

    try:
        out = session_apis["exec_shell"](f"kubectl get endpoints -n {namespace}")
        save_text(scenario_dir / f"{stage}_endpoints.txt", out)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_endpoints_error.txt", e)

    try:
        out = session_apis["exec_shell"](f"kubectl get events -n {namespace} --sort-by='.lastTimestamp'")
        save_text(scenario_dir / f"{stage}_events.txt", out)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_events_error.txt", e)

    try:
        logs = session_apis["get_logs"](namespace=namespace, service=service)
        save_text(scenario_dir / f"{stage}_logs.txt", logs)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_logs_error.txt", e)

    try:
        metrics_path = session_apis["get_metrics"](namespace=namespace, duration=5)
        save_text(scenario_dir / f"{stage}_metrics_path.txt", metrics_path)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_metrics_error.txt", e)

    try:
        traces_path = session_apis["get_traces"](namespace=namespace, duration=5)
        save_text(scenario_dir / f"{stage}_traces_path.txt", traces_path)
    except Exception as e:
        save_text(scenario_dir / f"{stage}_traces_error.txt", e)

async def run_one(problem_id: str):
    orch = Orchestrator()
    problem_desc, instructs, apis = orch.init_problem(problem_id)

    scenario_dir = OUT_ROOT / problem_id
    scenario_dir.mkdir(parents=True, exist_ok=True)

    save_text(scenario_dir / "problem_desc.txt", problem_desc)
    save_text(scenario_dir / "instructions.txt", instructs)
    save_text(scenario_dir / "apis.txt", apis)

    # You may want to parse namespace/service from spec later.
    # For now this is a placeholder default.
    namespace = "test-hotel-reservation"
    service = "frontend"

    # Before
    await snapshot_api(orch.apis, scenario_dir, namespace, service, "before")

    # Start scenario
    await orch.start_problem(max_steps=5)

    # During
    await snapshot_api(orch.apis, scenario_dir, namespace, service, "during")

    # Small delay for post-fault telemetry stabilization
    time.sleep(10)

    # After
    await snapshot_api(orch.apis, scenario_dir, namespace, service, "after")

async def main():
    ids = load_problem_ids(limit=50)   # start small
    for pid in ids:
        print(f"Running {pid}")
        try:
            await run_one(pid)
        except Exception as e:
            err = OUT_ROOT / pid / "run_error.txt"
            err.parent.mkdir(parents=True, exist_ok=True)
            err.write_text(str(e))

if __name__ == "__main__":
    asyncio.run(main())
