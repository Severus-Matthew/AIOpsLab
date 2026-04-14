from pathlib import Path
from aiopslab.orchestrator.problems.generated.generic_problem import build_generated_problem_class

def load_generated_registry(spec_root="scenario_specs"):
    registry = {}
    for json_path in Path(spec_root).rglob("*.json"):
        cls = build_generated_problem_class(str(json_path))
        registry[cls.__name__] = cls
    return registry
