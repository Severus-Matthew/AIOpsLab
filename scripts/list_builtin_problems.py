from aiopslab.orchestrator.problems.registry import ProblemRegistry

r = ProblemRegistry()
ids = r.get_problem_ids()

# Heuristic: built-ins do not contain workload-pattern tokens like steady_/burst_/diurnal
builtin = [x for x in ids if not any(tok in x for tok in ["steady_", "burst_", "diurnal"])]

print(f"builtin_count={len(builtin)}")
for x in builtin:
    print(x)
