import json
from pathlib import Path

from aiopslab.orchestrator.tasks.detection import DetectionTask
from aiopslab.orchestrator.tasks.localization import LocalizationTask
from aiopslab.orchestrator.tasks.analysis import AnalysisTask
from aiopslab.orchestrator.tasks.mitigation import MitigationTask
from aiopslab.orchestrator.evaluators.quantitative import (
    is_exact_match,
    is_subset,
    is_exact_match_lower,
)

from aiopslab.service.kubectl import KubeCtl
from aiopslab.service.apps.hotelres import HotelReservation
from aiopslab.generators.workload.wrk import Wrk
from aiopslab.generators.fault.inject_app import ApplicationFaultInjector
from aiopslab.session import SessionItem
from aiopslab.paths import TARGET_MICROSERVICES
from aiopslab.orchestrator.problems.misconfig_app.helpers import get_frontend_url


TASK_MAP = {
    "detection": DetectionTask,
    "localization": LocalizationTask,
    "analysis": AnalysisTask,
    "mitigation": MitigationTask,
}

# For now, only generated hotelres scenarios are truly runnable end-to-end.
APP_MAP = {
    "hotelres": HotelReservation,
}

SUPPORTED_GENERATED_FAULTS = {
    "hotelres": {
        "app_misconfig",
    }
}


class GeneratedProblemBase:
    def __init__(self, spec_path: str):
        self.spec_path = spec_path
        self.spec = json.loads(Path(spec_path).read_text())

        app_name = self.spec["app"]
        fault_type = self.spec["fault"]["type"]
        target = self.spec["fault"]["target"]

        if app_name not in APP_MAP:
            raise ValueError(f"Generated app not yet supported at runtime: {app_name}")
        if fault_type not in SUPPORTED_GENERATED_FAULTS.get(app_name, set()):
            raise ValueError(f"Generated fault not yet supported at runtime for {app_name}: {fault_type}")

        self.app = APP_MAP[app_name]()
        self.kubectl = KubeCtl()
        self.namespace = self.app.namespace
        self.faulty_service = target

        # Known-good payload from working hotelres family.
        self.payload_script = (
            TARGET_MICROSERVICES
            / "hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        )

    def start_workload(self):
        print("== Start Workload (Generated) ==")
        frontend_url = get_frontend_url(self.app)

        wrk = Wrk(
            rate=self.spec["workload"]["rate"],
            dist="exp",
            connections=2,
            duration=10,
            threads=2,
        )
        wrk.start_workload(
            payload_script=self.payload_script,
            url=f"{frontend_url}",
        )

    def inject_fault(self):
        print("== Fault Injection (Generated) ==")
        fault_type = self.spec["fault"]["type"]

        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._inject(
            fault_type=fault_type,
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    def recover_fault(self):
        print("== Fault Recovery (Generated) ==")
        fault_type = self.spec["fault"]["type"]

        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._recover(
            fault_type=fault_type,
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    def _eval_detection(self, soln, trace, duration):
        expected_solution = "Yes"
        if isinstance(soln, str):
            if soln.strip().lower() == expected_solution.lower():
                self.add_result("Detection Accuracy", "Correct")
            else:
                self.add_result("Detection Accuracy", "Incorrect")
        else:
            self.add_result("Detection Accuracy", "Invalid Format")
        return super().eval(soln, trace, duration)

    def _eval_localization(self, soln, trace, duration):
        if soln is None:
            self.add_result("Localization Accuracy", 0.0)
            self.results["success"] = False
            self.results["is_subset"] = False
            super().eval(soln, trace, duration)
            return self.results

        is_exact = is_exact_match(soln, self.faulty_service)
        is_sub = is_subset([self.faulty_service], soln)

        if is_exact:
            accuracy = 100.0
        elif is_sub:
            accuracy = (len([self.faulty_service]) / len(soln)) * 100.0
        else:
            accuracy = 0.0

        self.add_result("Localization Accuracy", accuracy)
        super().eval(soln, trace, duration)

        self.results["success"] = is_exact or (is_sub and len(soln) == 1)
        self.results["is_subset"] = is_sub
        return self.results

    def _eval_analysis(self, soln, trace, duration):
        if not isinstance(soln, dict):
            self.results["system_level_correct"] = False
            self.results["fault_type_correct"] = False
            self.results["success"] = False
            super().eval(soln, trace, duration)
            return self.results

        is_sys_level_correct = is_exact_match_lower(
            soln.get("system_level", ""), "Application"
        )
        is_fault_type_correct = is_exact_match_lower(
            soln.get("fault_type", ""), "Misconfiguration"
        )

        self.results["system_level_correct"] = is_sys_level_correct
        self.results["fault_type_correct"] = is_fault_type_correct
        self.results["success"] = is_sys_level_correct and is_fault_type_correct

        super().eval(soln, trace, duration)
        return self.results

    def _eval_mitigation(self, soln, trace, duration):
        super().eval(soln, trace, duration)
        try:
            self.kubectl.wait_for_ready(self.namespace, sleep=5, max_wait=60)
            self.results["success"] = True
        except Exception:
            self.results["success"] = False
        return self.results


def build_generated_problem_class(spec_path: str):
    spec = json.loads(Path(spec_path).read_text())
    task_cls = TASK_MAP[spec["task"]]

    class GeneratedProblem(GeneratedProblemBase, task_cls):
        def __init__(self):
            GeneratedProblemBase.__init__(self, spec_path)
            task_cls.__init__(self, self.app)

        def eval(self, soln, trace: list[SessionItem], duration: float):
            self.add_result("scenario_id", self.spec["scenario_id"])
            self.add_result("fault_type", self.spec["fault"]["type"])
            self.add_result("severity", self.spec["fault"]["severity"])
            self.add_result("target", self.spec["fault"]["target"])

            if self.spec["task"] == "detection":
                return self._eval_detection(soln, trace, duration)
            if self.spec["task"] == "localization":
                return self._eval_localization(soln, trace, duration)
            if self.spec["task"] == "analysis":
                return self._eval_analysis(soln, trace, duration)
            if self.spec["task"] == "mitigation":
                return self._eval_mitigation(soln, trace, duration)

            return super().eval(soln, trace, duration)

    GeneratedProblem.__name__ = spec["scenario_id"]
    return GeneratedProblem
