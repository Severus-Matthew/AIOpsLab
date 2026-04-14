import asyncio
import json
import shutil
import time
from pathlib import Path

from aiopslab.orchestrator import Orchestrator
from aiopslab.orchestrator.problems.registry import ProblemRegistry

OUT_ROOT = Path("dataset/raw_existing")
RESULTS_DIR = Path("aiopslab/data/results")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

def save_text(path: Path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content))

def list_builtin_problem_ids():
    r = ProblemRegistry()
    ids = r.get_problem_ids()

    builtin = [x for x in ids if not any(tok in x for tok in ["steady_", "burst_", "diurnal"])]

    keep = []
    for x in builtin:
        if any(tok in x for tok in [
            "hotel_res",
            "hotel_reservation",
            "misconfig_app_hotel_res",
            "network_delay_hotel_res",
            "network_loss_hotel_res",
            "pod_failure_hotel_res",
            "pod_kill_hotel_res",
            "redeploy_without_PV",
            "revoke_auth_mongodb",
            "user_unregistered_mongodb",
            "wrong_bin_usage",
            "container_kill",
        ]):
            keep.append(x)

    return sorted(keep)

def latest_result_file():
    files = sorted(RESULTS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None

async def snapshot(session_apis, scenario_dir: Path, namespace: str, service: str, stage: str):
    cmds = {
        f"{stage}_pods.txt": f"kubectl get pods -n {namespace} -o wide",
        f"{stage}_svc.txt": f"kubectl get svc -n {namespace}",
        f"{stage}_endpoints.txt": f"kubectl get endpoints -n {namespace}",
        f"{stage}_events.txt": f"kubectl get events -n {namespace} --sort-by='.lastTimestamp'",
    }

    for fname, cmd in cmds.items():
        try:
            out = session_apis["exec_shell"](cmd)
            save_text(scenario_dir / fname, out)
        except Exception as e:
            save_text(scenario_dir / fname.replace(".txt", "_error.txt"), e)

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

def infer_namespace_and_service(problem_id: str):
    # Conservative defaults based on built-in families
    if "hotel_res" in problem_id or "hotel_reservation" in problem_id or "hotelres" in problem_id:
        return "test-hotel-reservation", "geo"
    if "social_net" in problem_id or "social_network" in problem_id or "socialnet" in problem_id:
        return "social-network", "user-service"
    if "astronomy_shop" in problem_id:
        return "astronomy-shop", "frontend"
    if "flower" in problem_id:
        return "default", "frontend"
    if "operator_" in problem_id or "tidb" in problem_id:
        return "tidb-cluster", "operator"
    return "default", "frontend"

async def run_one(problem_id: str):
    orch = Orchestrator()
    orch.agent_name = "telemetry_collector"
    problem_desc, instructs, apis = orch.init_problem(problem_id)

    scenario_dir = OUT_ROOT / problem_id
    scenario_dir.mkdir(parents=True, exist_ok=True)

    save_text(scenario_dir / "problem_desc.txt", problem_desc)
    save_text(scenario_dir / "instructions.txt", instructs)
    save_text(scenario_dir / "apis.txt", apis)

    namespace, service = infer_namespace_and_service(problem_id)

    # Before
    await snapshot(orch.apis, scenario_dir, namespace, service, "before")

    # Start problem
    await orch.start_problem(max_steps=5)

    # During
    await snapshot(orch.apis, scenario_dir, namespace, service, "during")

    time.sleep(10)

    # After
    await snapshot(orch.apis, scenario_dir, namespace, service, "after")

    # Save latest evaluator result file if present
    rf = latest_result_file()
    if rf is not None:
        shutil.copy2(rf, scenario_dir / "result.json")

async def main():
    ids = list_builtin_problem_ids()

    # Start with a pilot subset first
    pilot = ["misconfig_app_hotel_res-detection-1"]
    print(f"Running {len(pilot)} built-in problems")
    for pid in pilot:
        print(f"=== {pid} ===")
        try:
            await run_one(pid)
        except Exception as e:
            err = OUT_ROOT / pid / "run_error.txt"
            err.parent.mkdir(parents=True, exist_ok=True)
            err.write_text(str(e))

if __name__ == "__main__":
    asyncio.run(main())
