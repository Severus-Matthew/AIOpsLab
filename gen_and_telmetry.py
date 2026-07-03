import sys
if not (3, 11) <= sys.version_info < (3, 13):
    raise RuntimeError(
        f"Python 3.11 or 3.12 required (got {sys.version}). "
        "Run with: .venv312/bin/python gen_and_telmetry.py"
    )

import asyncio
import csv
import importlib
import json
import traceback
from pathlib import Path
from typing import Callable, Dict, List, Any
import inspect
import itertools
import types
import subprocess
import os
import time
from datetime import datetime

# Give pods up to 20 min to become ready. 900s was too tight after cluster
# restarts (cold images on worker node + init-container git clones).
# Can be overridden: AIOPSLAB_POD_READY_TIMEOUT=1200 python gen_and_telmetry.py
os.environ.setdefault("AIOPSLAB_POD_READY_TIMEOUT", "1200")

WAIT_AFTER_FAULT_SEC = 20
# Per-scenario timeout: OpenEBS wait (skipped fast now) + helm deploy + telemetry
TIMEOUT_SEC = 2400

from aiopslab.orchestrator import Orchestrator


# ==========================================================
# CONFIG
# ==========================================================

# Parallel sharding via env vars — maps cleanly to SLURM job arrays:
#   AIOPSLAB_WORKER_ID   = $SLURM_ARRAY_TASK_ID  (0-based, default 0)
#   AIOPSLAB_NUM_WORKERS = $SLURM_ARRAY_TASK_COUNT (default 1 = no sharding)
# Each worker processes all_specs[worker_id::num_workers], so the scenario
# list is striped across workers with no overlap.
_WORKER_ID = int(os.environ.get("AIOPSLAB_WORKER_ID", "0"))
_NUM_WORKERS = int(os.environ.get("AIOPSLAB_NUM_WORKERS", "1"))

OUT_DIR = Path(
    os.environ.get(
        "AIOPSLAB_OUT_DIR",
        "dynamic_generated_scenario_results_all_new"
    )
)
PASSED_DIR = OUT_DIR / "passed"
FAILED_DIR = OUT_DIR / "failed"
SPECS_DIR = OUT_DIR / "specs"
LOG_FILE = OUT_DIR / "log.jsonl"
TELEMETRY_DIR = OUT_DIR / "telemetry_outputs"




MAX_GENERATED = 7000
BATCH_SIZE = 1
MAX_STEPS = 1
# TIMEOUT_SEC = 300

# Set True only if you want to try unstable / known-bug categories.
ENABLE_EXPERIMENTAL_BROKEN = False


# ==========================================================
# SERVICE CANDIDATES
# ==========================================================

SOCIAL_SERVICES = [
    "user-service",
    "text-service",
    "post-storage-service",
    "user-timeline-service",
    "home-timeline-service",
    "social-graph-service",
    "media-service",
    "unique-id-service",
    "compose-post-service",
    "url-shorten-service",
    "user-mongodb",
    "post-storage-mongodb",
    "social-graph-mongodb",
    "url-shorten-mongodb",
    "user-timeline-mongodb",
    "home-timeline-redis",
    "user-mention-service",
    "media-memcached",
    "post-storage-memcached",
    "social-graph-redis",
]

SOCIAL_DEPLOYMENT_SERVICES = [
    "user-service",
    "text-service",
    "post-storage-service",
    "user-timeline-service",
    "home-timeline-service",
    "social-graph-service",
    "media-service",
    "unique-id-service",
    "compose-post-service",
    "url-shorten-service",
]

SOCIAL_MONGO_SERVICES = [
    "url-shorten-mongodb",
    "user-mongodb",
    "post-storage-mongodb",
    "social-graph-mongodb",
    "user-timeline-mongodb",
]

HOTEL_SERVICES = [
    "user",
    "geo",
    "profile",
    "rate",
    "recommendation",
    "reservation",
    "search",
    "frontend",
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
    "memcached-rate",
    "memcached-profile",
]

HOTEL_APP_SERVICES = [
    "user",
    "geo",
    "profile",
    "rate",
    "recommendation",
    "reservation",
    "search",
    "frontend",
]

HOTEL_MONGO_SERVICES = [
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
]

FLOWER_SERVICES = [
    "supernode-1",
    "supernode-2",
    "clientapp-1",
    "clientapp-2",
    "serverapp",
]

NOOP_APPS = [
    "hotel",
    "social",
    # "astronomy_shop",
]


# ==========================================================
# TASK / FAMILY DEFINITIONS
# ==========================================================

def task(folder_name: str, py_file_name: str, class_name: str) -> Dict[str, Any]:
    return {
        "folder_name": folder_name,
        "py_file_name": py_file_name,
        "class_name": class_name,
    }


FAULT_FAMILIES = {
    # ======================================================
    # SocialNetwork / virtualization / app faults
    # ======================================================
    "k8s_target_port_misconfig": {
        "app": "social",
        "mode": "constructor_service",
        "services": SOCIAL_DEPLOYMENT_SERVICES,
        "tasks": {
            "detection": task("k8s_target_port_misconfig", "target_port","K8STargetPortMisconfigDetection"),
            "localization": task("k8s_target_port_misconfig", "target_port","K8STargetPortMisconfigLocalization"),
            "analysis": task("k8s_target_port_misconfig", "target_port","K8STargetPortMisconfigAnalysis"),
            "mitigation": task("k8s_target_port_misconfig", "target_port","K8STargetPortMisconfigMitigation"),
        },
    },

    "auth_miss_mongodb": {
        "app": "social",
        "mode": "override_service",
        "services": SOCIAL_MONGO_SERVICES,
        "tasks": {
            "detection": task("auth_miss_mongodb", "auth_miss_mongodb", "MongoDBAuthMissingDetection"),
            "localization": task("auth_miss_mongodb", "auth_miss_mongodb", "MongoDBAuthMissingLocalization"),
            "analysis": task("auth_miss_mongodb", "auth_miss_mongodb", "MongoDBAuthMissingAnalysis"),
            "mitigation": task("auth_miss_mongodb", "auth_miss_mongodb", "MongoDBAuthMissingMitigation"),
        },
    },

    "scale_pod_zero_social_net": {
        "app": "social",
        "mode": "override_service",
        "services": SOCIAL_DEPLOYMENT_SERVICES,
        "tasks": {
            "detection": task("scale_pod", "scale_pod_social_net", "ScalePodSocialNetDetection"),
            "localization": task("scale_pod", "scale_pod_social_net", "ScalePodSocialNetLocalization"),
            "analysis": task("scale_pod", "scale_pod_social_net", "ScalePodSocialNetAnalysis"),
            "mitigation": task("scale_pod", "scale_pod_social_net", "ScalePodSocialNetMitigation"),
        },
    },

    "assign_to_non_existent_node_social_net": {
        "app": "social",
        "mode": "override_service",
        "services": SOCIAL_DEPLOYMENT_SERVICES,
        "tasks": {
            "detection": task(
                "assign_non_existent_node", "assign_non_existent_node_social_net", "AssignNonExistentNodeSocialNetDetection"),
            "localization": task("assign_non_existent_node", "assign_non_existent_node_social_net", "AssignNonExistentNodeSocialNetLocalization"),
            "analysis": task("assign_non_existent_node", "assign_non_existent_node_social_net", "AssignNonExistentNodeSocialNetAnalysis"),
            "mitigation": task("assign_non_existent_node", "assign_non_existent_node_social_net", "AssignNonExistentNodeSocialNetMitigation"),
        },
    },

    # ======================================================
    # HotelReservation / MongoDB app faults
    # ======================================================
    "revoke_auth_mongodb": {
        "app": "hotel",
        "mode": "constructor_service",
        "services": HOTEL_MONGO_SERVICES,
        "tasks": {
            "detection": task("revoke_auth", "revoke_auth", "MongoDBRevokeAuthDetection"),
            "localization": task("revoke_auth", "revoke_auth", "MongoDBRevokeAuthLocalization"),
            "analysis": task("revoke_auth", "revoke_auth", "MongoDBRevokeAuthAnalysis"),
            "mitigation": task("revoke_auth", "revoke_auth", "MongoDBRevokeAuthMitigation"),
        },
    },

    "user_unregistered_mongodb": {
        "app": "hotel",
        "mode": "constructor_service",
        "services": HOTEL_MONGO_SERVICES,
        "tasks": {
            "detection": task("storage_user_unregistered", "storage_user_unregistered", "MongoDBUserUnregisteredDetection"),
            "localization": task("storage_user_unregistered", "storage_user_unregistered", "MongoDBUserUnregisteredLocalization"),
            "analysis": task("storage_user_unregistered", "storage_user_unregistered", "MongoDBUserUnregisteredAnalysis"),
            "mitigation": task("storage_user_unregistered", "storage_user_unregistered", "MongoDBUserUnregisteredMitigation"),
        },
    },

    "misconfig_app_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("misconfig_app", "misconfig_app_hotel_res", "MisconfigAppHotelResDetection"),
            "localization": task("misconfig_app", "misconfig_app_hotel_res", "MisconfigAppHotelResLocalization"),
            "analysis": task("misconfig_app", "misconfig_app_hotel_res", "MisconfigAppHotelResAnalysis"),
            "mitigation": task("misconfig_app", "misconfig_app_hotel_res", "MisconfigAppHotelResMitigation"),
        },
    },

    "wrong_bin_usage": {
        "app": "hotel",
        "mode": "constructor_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("wrong_bin_usage", "wrong_bin_usage", "WrongBinUsageDetection"),
            "localization": task("wrong_bin_usage", "wrong_bin_usage", "WrongBinUsageLocalization"),
            "analysis": task("wrong_bin_usage", "wrong_bin_usage", "WrongBinUsageAnalysis"),
            "mitigation": task("wrong_bin_usage", "wrong_bin_usage", "WrongBinUsageMitigation"),
        },
    },

    "redeploy_without_pv": {
        "app": "hotel",
        "mode": "fixed",
        "tasks": {
            "detection": task("redeploy_without_pv", "redeploy_without_pv", "RedeployWithoutPVDetection"),
            "analysis": task("redeploy_without_pv", "redeploy_without_pv", "RedeployWithoutPVAnalysis"),
            "mitigation": task("redeploy_without_pv", "redeploy_without_pv", "RedeployWithoutPVMitigation"),
        },
    },

    # ======================================================
    # HotelReservation / symptom faults
    # # ======================================================
    "network_loss_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("network_loss", "network_loss", "NetworkLossDetection"),
            "localization": task("network_loss", "network_loss", "NetworkLossLocalization"),
        },
    },

    "network_delay_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("network_delay", "network_delay", "NetworkDelayDetection"),
            "localization": task("network_delay", "network_delay", "NetworkDelayLocalization"),
        },
    },

    "pod_failure_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("pod_failure", "pod_failure", "PodFailureDetection"),
            "localization": task("pod_failure", "pod_failure", "PodFailureLocalization"),
        },
    },

    "pod_kill_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("pod_kill", "pod_kill", "PodKillDetection"),
            "localization": task("pod_kill", "pod_kill", "PodKillLocalization"),
        },
    },

    "container_kill_hotel_res": {
        "app": "hotel",
        "mode": "override_service",
        "services": HOTEL_APP_SERVICES,
        "tasks": {
            "detection": task("container_kill", "container_kill", "ContainerKillDetection"),
            "localization": task("container_kill", "container_kill", "ContainerKillLocalization"),
        },
    },

    # ======================================================
    # AstronomyShop / OpenTelemetry Demo faults
    # These are fixed feature-flag faults. Do not vary service.
    # ======================================================
    "astronomy_shop_ad_service_failure": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("ad_service_failure", "ad_service_failure", "AdServiceFailureDetection"),
            "localization": task("ad_service_failure", "ad_service_failure", "AdServiceFailureLocalization"),
        },
    },

    "astronomy_shop_ad_service_high_cpu": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("ad_service_high_cpu", "ad_service_high_cpu", "AdServiceHighCpuDetection"),
            "localization": task("ad_service_high_cpu", "ad_service_high_cpu", "AdServiceHighCpuLocalization"),
        },
    },

    "astronomy_shop_ad_service_manual_gc": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("ad_service_manual_gc", "ad_service_manual_gc", "AdServiceManualGcDetection"),
            "localization": task("ad_service_manual_gc", "ad_service_manual_gc", "AdServiceManualGcLocalization"),
        },
    },

    "astronomy_shop_cart_service_failure": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("cart_service_failure", "cart_service_failure", "CartServiceFailureDetection"),
            "localization": task("cart_service_failure", "cart_service_failure", "CartServiceFailureLocalization"),
        },
    },

    "astronomy_shop_image_slow_load": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("image_slow_load", "image_slow_load", "ImageSlowLoadDetection"),
            "localization": task("image_slow_load", "image_slow_load", "ImageSlowLoadLocalization"),
        },
    },

    "astronomy_shop_kafka_queue_problems": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("kafka_queue_problems", "kafka_queue_problems", "KafkaQueueProblemsDetection"),
            "localization": task("kafka_queue_problems", "kafka_queue_problems", "KafkaQueueProblemsLocalization"),
            "mitigation": task("kafka_queue_problems", "kafka_queue_problems", "KafkaQueueProblemsMitigation"),
        },
    },

    "astronomy_shop_loadgenerator_flood_homepage": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("loadgenerator_flood_homepage", "loadgenerator_flood_homepage", "LoadGeneratorFloodHomepageDetection"),
            "localization": task("loadgenerator_flood_homepage", "loadgenerator_flood_homepage", "LoadGeneratorFloodHomepageLocalization"),
        },
    },

    "astronomy_shop_payment_service_failure": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("payment_service_failure", "payment_service_failure", "PaymentServiceFailureDetection"),
            "localization": task("payment_service_failure", "payment_service_failure", "PaymentServiceFailureLocalization"),
        },
    },

    "astronomy_shop_payment_service_unreachable": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("payment_service_unreachable", "payment_service_unreachable", "PaymentServiceUnreachableDetection"),
            "localization": task("payment_service_unreachable", "payment_service_unreachable", "PaymentServiceUnreachableLocalization"),
        },
    },

    "astronomy_shop_product_catalog_service_failure": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("product_catalog_failure", "product_catalog_failure", "ProductCatalogServiceFailureDetection"),
            "localization": task("product_catalog_failure", "product_catalog_failure", "ProductCatalogServiceFailureLocalization"),
        },
    },

    "astronomy_shop_recommendation_service_cache_failure": {
        "app": "astronomy",
        "mode": "fixed",
        "tasks": {
            "detection": task("recommendation_service_cache_failure", "recommendation_service_cache_failure", "RecommendationServiceCacheFailureDetection"),
            "localization": task("recommendation_service_cache_failure", "recommendation_service_cache_failure", "RecommendationServiceCacheFailureLocalization"),
        },
    },

    # ======================================================
    # Flower / Docker faults
    # ======================================================
    "flower_node_stop": {
        "app": "flower",
        "mode": "constructor_service",
        "services": FLOWER_SERVICES,
        "deployment": "docker",
        "tasks": {
            "detection": task("flower_node_stop", "node_stop", "FlowerNodeStopDetection"),
        },
    },

    "flower_model_misconfig": {
        "app": "flower",
        "mode": "constructor_service",
        "services": FLOWER_SERVICES,
        "deployment": "docker",
        "tasks": {
            "detection": task("flower_model_misconfig", "model_misconfig", "FlowerModelMisconfigDetection"),
        },
    },

    # ======================================================
    # TiDB operator faults
    # ======================================================
    "operator_non_existent_storage": {
        "app": "tidb",
        "mode": "fixed",
        "tasks": {
            "detection": task("operator_misoperation", "non_existent_storage", "K8SOperatorNonExistentStorageDetection"),
            "localization": task("operator_misoperation", "non_existent_storage", "K8SOperatorNonExistentStorageLocalization"),
        },
    },

    "operator_overload_replicas": {
        "app": "tidb",
        "mode": "fixed",
        "tasks": {
            "detection": task("operator_misoperation", "overload_replicas", "K8SOperatorOverloadReplicasDetection"),
            "localization": task("operator_misoperation", "overload_replicas", "K8SOperatorOverloadReplicasLocalization"),
        },
    },

    "operator_security_context_fault": {
        "app": "tidb",
        "mode": "fixed",
        "tasks": {
            "detection": task("operator_misoperation", "security_context_fault", "K8SOperatorSecurityContextFaultDetection"),
            "localization": task("operator_misoperation", "security_context_fault", "K8SOperatorSecurityContextFaultLocalization"),
        },
    },

    "operator_wrong_update_strategy": {
        "app": "tidb",
        "mode": "fixed",
        "tasks": {
            "detection": task("operator_misoperation", "wrong_update_strategy", "K8SOperatorWrongUpdateStrategyDetection"),
            "localization": task("operator_misoperation", "wrong_update_strategy", "K8SOperatorWrongUpdateStrategyLocalization"),
        },
    },

    "operator_invalid_affinity_toleration": {
        "app": "tidb",
        "mode": "fixed",
        "tasks": {
            "detection": task("operator_misoperation", "invalid_affinity_toleration", "K8SOperatorInvalidAffinityTolerationDetection"),
            "localization": task("operator_misoperation", "invalid_affinity_toleration", "K8SOperatorInvalidAffinityTolerationLocalization"),
        },
    },

    # ======================================================
    # No-op false-positive checks
    # ======================================================
    "noop_detection": {
        "app": "noop",
        "mode": "constructor_app",
        "apps": NOOP_APPS,
        "tasks": {
            "detection": task("no_op","no_op", "NoOpDetection"),
        },
    },
}


if ENABLE_EXPERIMENTAL_BROKEN:
    FAULT_FAMILIES.update(
        {
            "kernel_fault_hotel_res": {
                "mode": "override_service",
                "services": HOTEL_APP_SERVICES,
                "tasks": {
                    "detection": task("kernel_fault", "kernel_fault", "KernelFaultDetection"),
                    "localization": task("kernel_fault", "kernel_fault", "KernelFaultLocalization"),
                },
            },
            "disk_woreout_hotel_res": {
                "mode": "fixed",
                "tasks": {
                    "detection": task("disk_woreout", "disk_woreout", "DiskWoreoutDetection"),
                    "localization": task("disk_woreout", "disk_woreout", "DiskWoreoutLocalization"),
                },
            },
        }
    )

READINESS_BREAKING_FAULTS = {
    "assign_to_non_existent_node_social_net",
    "scale_pod_zero_social_net",
    "pod_failure_hotel_res",
    "pod_kill_hotel_res",
    "container_kill_hotel_res",
    "operator_non_existent_storage",
    "operator_overload_replicas",
    "operator_security_context_fault",
    "operator_wrong_update_strategy",
    "operator_invalid_affinity_toleration",
}

VARIANT_CANDIDATES = {
    # Only applied if the class constructor supports these kwargs.
    "generic_duration": [
        {"variant": "default"},
        {"variant": "short", "duration": 30},
        {"variant": "medium", "duration": 60},
        {"variant": "long", "duration": 120},
    ],

    "generic_intensity": [
        {"variant": "default"},
        {"variant": "low", "intensity": "low"},
        {"variant": "medium", "intensity": "medium"},
        {"variant": "high", "intensity": "high"},
    ],

    "network_delay": [
        {"variant": "default"},
        {"variant": "delay_100ms", "delay_ms": 100},
        {"variant": "delay_300ms", "delay_ms": 300},
        {"variant": "delay_1000ms", "delay_ms": 1000},
    ],

    "network_loss": [
        {"variant": "default"},
        {"variant": "loss_5pct", "loss_percent": 5},
        {"variant": "loss_20pct", "loss_percent": 20},
        {"variant": "loss_50pct", "loss_percent": 50},
    ],

    "scale_pod": [
        {"variant": "default"},
        {"variant": "scale_0", "replicas": 0},
        {"variant": "scale_2", "replicas": 2},
        {"variant": "scale_3", "replicas": 3},
    ],
}


def get_constructor_param_names(cls):
    try:
        sig = inspect.signature(cls.__init__)
        return set(sig.parameters.keys()) - {"self"}
    except Exception:
        return set()


def class_supports_kwargs(cls, kwargs):
    params = get_constructor_param_names(cls)

    # If constructor has **kwargs, assume it supports variants.
    try:
        sig = inspect.signature(cls.__init__)
        for p in sig.parameters.values():
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return True
    except Exception:
        pass

    return all(k in params for k in kwargs if k != "variant")

def get_expected_faulty_services(spec):
    if spec.get("is_multifault"):
        return {
            str(sp.get("faulty_service"))
            for sp in spec.get("subproblems", [])
            if sp.get("faulty_service")
        }

    if spec.get("faulty_service"):
        return {str(spec["faulty_service"])}

    return set()


def get_supported_variants_for_task(task_cfg, fault_name):
    cls = import_problem_class(
        task_cfg["folder_name"],
        task_cfg["py_file_name"],
        task_cfg["class_name"],
    )

    candidates = [{"variant": "default"}]

    lname = fault_name.lower()

    # These families use post-construction override — all their variants are supported.
    POST_CONSTRUCTION_VARIANT_FAMILIES = ("network_delay", "network_loss", "scale_pod")

    if "network_delay" in lname:
        return VARIANT_CANDIDATES["network_delay"]
    elif "network_loss" in lname:
        return VARIANT_CANDIDATES["network_loss"]
    elif "scale_pod" in lname:
        return VARIANT_CANDIDATES["scale_pod"]
    else:
        # Try general variants only if constructor supports them.
        candidates = (
            VARIANT_CANDIDATES["generic_duration"]
            + VARIANT_CANDIDATES["generic_intensity"][1:]
        )

    supported = []

    for v in candidates:
        kwargs = {k: val for k, val in v.items() if k != "variant"}
        if not kwargs or class_supports_kwargs(cls, kwargs):
            supported.append(v)

    # Avoid duplicate default.
    unique = {}
    for v in supported:
        unique[v["variant"]] = v

    return list(unique.values())

# ==========================================================
# DUMMY AGENT
# ==========================================================

class BuiltinFullCollectorAgent:
    def __init__(self, task: str, namespace: str = "default", faulty_service: str | None = None):
        self.task = task
        self.namespace = namespace
        self.faulty_service = faulty_service
        self.actions: List[str] = []
        self.records: List[Dict[str, Any]] = []
        self.step = 0

    def _discover_services(self) -> List[str]:
        res = run_cmd(
            f"kubectl get svc -n {self.namespace} "
            "-o jsonpath='{.items[*].metadata.name}'",
            timeout=60,
        )

        services = res["stdout"].strip().replace("'", "").split()

        if self.faulty_service and self.faulty_service in services:
            services.remove(self.faulty_service)
            services.insert(0, self.faulty_service)

        return services

    def configure_actions(self, apis: Dict[str, Any]):
        names = list(apis.keys())
        actions: List[str] = []

        services = self._discover_services()

        if "get_logs" in names:
            for svc in services:
                actions.append(
                    f'get_logs(namespace="{self.namespace}", service="{svc}")'
                )

        if "get_metrics" in names:
            actions.append(
                f'get_metrics(namespace="{self.namespace}", duration=5)'
            )

        if "get_traces" in names:
            actions.append(
                f'get_traces(namespace="{self.namespace}", duration=5)'
            )

        if "get_microservice_repo_diff" in names:
            actions.append("get_microservice_repo_diff()")

        if "exec_shell" in names:
            shell_cmds = [
                f"kubectl get pods -n {self.namespace} -o wide",
                f"kubectl get pods -n {self.namespace} -o json",
                f"kubectl get svc -n {self.namespace} -o wide",
                f"kubectl get svc -n {self.namespace} -o json",
                f"kubectl get deploy -n {self.namespace} -o wide",
                f"kubectl get deploy -n {self.namespace} -o json",
                f"kubectl get rs -n {self.namespace} -o json",
                f"kubectl get endpoints -n {self.namespace} -o json",
                f"kubectl get events -n {self.namespace} --sort-by=.lastTimestamp",
                f"kubectl describe pods -n {self.namespace}",
                f"kubectl top pods -n {self.namespace} || true",
                "kubectl top nodes || true",
            ]

            for cmd in shell_cmds:
                escaped = cmd.replace('"', '\\"')
                actions.append(
                    f'exec_shell(command="{escaped}", timeout=120)'
                )

        if self.task == "detection":
            actions.append('submit("Yes")')
        elif self.task == "localization":
            if self.faulty_service:
                actions.append(f'submit(["{self.faulty_service}"])')
            else:
                actions.append('submit(["unknown"])')
        elif self.task == "analysis":
            actions.append(
                'submit({"system_level": "unknown", "fault_type": "unknown"})'
            )
        else:
            actions.append('submit("unknown")')

        seen = set()
        self.actions = []
        for a in actions:
            if a not in seen:
                self.actions.append(a)
                seen.add(a)

    async def get_action(self, env_input: str) -> str:
        if self.step > 0 and len(self.records) >= self.step:
            self.records[self.step - 1]["output"] = env_input

        if self.step < len(self.actions):
            action = self.actions[self.step]
        else:
            action = 'submit("unknown")'

        self.records.append(
            {
                "step": self.step,
                "action": action,
                "output": None,
            }
        )

        self.step += 1

        return f"Action:\n```\n{action}\n```"


# ==========================================================
# HELPERS
# ==========================================================

def mkdirs():
    for d in [OUT_DIR, PASSED_DIR, FAILED_DIR, SPECS_DIR, TELEMETRY_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def safe_name(x: str) -> str:
    return (
        x.replace("/", "_")
        .replace(" ", "_")
        .replace(":", "_")
        .replace("|", "_")
        .replace("(", "_")
        .replace(")", "_")
    )


def result_path_exists(problem_id: str) -> bool:
    """Return True only if a CURRENT-run result with the new data files already exists.

    Old results from previous run directories are intentionally NOT skipped here,
    because those directories pre-date ground_truth.json / fault_timing.json.
    A scenario is considered done only when its telemetry folder inside the current
    OUT_DIR contains both a DONE marker AND ground_truth.json.
    """
    name = safe_name(problem_id)
    # Already in specs (queued this run) or failed this run — let failed ones retry
    if (SPECS_DIR / f"{name}.json").exists():
        return True
    # Passed this run AND has ground_truth.json (new-format result)
    telemetry_scenario_dir = TELEMETRY_DIR / name
    if (
        (PASSED_DIR / f"{name}.json").exists()
        and (telemetry_scenario_dir / "ground_truth.json").exists()
    ):
        return True
    return False


def write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)

def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", errors="replace") as f:
        f.write(text)

def run_cmd(cmd: str, timeout: int = 120) -> Dict[str, Any]:
    try:
        p = subprocess.run(
            cmd,
            shell=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        return {
            "cmd": cmd,
            "returncode": p.returncode,
            "stdout": p.stdout,
            "stderr": p.stderr,
        }
    except Exception as e:
        return {
            "cmd": cmd,
            "returncode": None,
            "stdout": "",
            "stderr": repr(e),
        }

def append_log(obj: Any):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(obj) + "\n")


def import_problem_class(folder_name: str, py_file_name: str, class_name: str):
    full_module = (
        f"aiopslab.orchestrator.problems."
        f"{folder_name}.{py_file_name}"
    )

    try:
        module = importlib.import_module(full_module)
        return getattr(module, class_name)
    except Exception as e:
        raise ImportError(
            f"\nCould not import problem class.\n"
            f"Folder: {folder_name}\n"
            f"Python file: {py_file_name}.py\n"
            f"Class: {class_name}\n"
            f"Tried module path: {full_module}\n"
            f"Original error: {repr(e)}"
        )


def _apply_variant_override(obj, variant_kwargs: Dict[str, Any], spec: Dict[str, Any]):
    """
    Post-construction override for variant parameters that aren't accepted by
    the problem class constructor. Patches inject_fault (and recover_fault
    where needed) so the fault is injected with the requested parameters.
    """
    if not variant_kwargs:
        return

    if "delay_ms" in variant_kwargs:
        delay_ms = variant_kwargs["delay_ms"]

        def _inject_fault(self):
            print("== Fault Injection ==")
            self.injector.inject_network_delay(
                [self.faulty_service], latency=f"{delay_ms}ms"
            )
            print(f"Service: {self.faulty_service} | delay: {delay_ms}ms | Namespace: {self.namespace}")

        obj.inject_fault = types.MethodType(_inject_fault, obj)

    elif "loss_percent" in variant_kwargs:
        loss_percent = variant_kwargs["loss_percent"]

        def _inject_fault(self):
            print("== Fault Injection ==")
            chaos_experiment = {
                "apiVersion": "chaos-mesh.org/v1alpha1",
                "kind": "NetworkChaos",
                "metadata": {"name": "loss", "namespace": self.namespace},
                "spec": {
                    "action": "loss",
                    "mode": "one",
                    "duration": "200s",
                    "selector": {
                        "labelSelectors": {"io.kompose.service": self.faulty_service}
                    },
                    "loss": {"loss": str(loss_percent), "correlation": "100"},
                },
            }
            self.injector.create_chaos_experiment(chaos_experiment, "network-loss")
            print(f"Service: {self.faulty_service} | loss: {loss_percent}% | Namespace: {self.namespace}")

        obj.inject_fault = types.MethodType(_inject_fault, obj)

    elif "replicas" in variant_kwargs:
        replicas = variant_kwargs["replicas"]

        def _inject_fault(self):
            print("== Fault Injection ==")
            subprocess.run(
                f"kubectl scale deployment {self.faulty_service} --replicas={replicas} -n {self.namespace}",
                shell=True, check=False,
            )
            if replicas == 0:
                time.sleep(30)
            print(f"Service: {self.faulty_service} | replicas: {replicas} | Namespace: {self.namespace}")

        def _recover_fault(self):
            print("== Fault Recovery ==")
            subprocess.run(
                f"kubectl scale deployment {self.faulty_service} --replicas=1 -n {self.namespace}",
                shell=True, check=False,
            )

        obj.inject_fault = types.MethodType(_inject_fault, obj)
        obj.recover_fault = types.MethodType(_recover_fault, obj)


def make_problem_factory(spec: Dict[str, Any]) -> Callable:
    cls = import_problem_class(
        spec["folder_name"],
        spec["py_file_name"],
        spec["class_name"],
    )

    mode = spec["mode"]
    variant = spec.get("variant", {"variant": "default"})
    variant_kwargs = {k: v for k, v in variant.items() if k != "variant"}

    def factory():
        # Pass variant kwargs to constructor only if it supports them.
        constructor_kwargs = variant_kwargs if class_supports_kwargs(cls, variant_kwargs) else {}

        if mode == "constructor_service":
            obj = cls(faulty_service=spec["faulty_service"], **constructor_kwargs)
        elif mode == "constructor_app":
            obj = cls(app_name=spec["app_name"], **constructor_kwargs)
        elif mode == "override_service":
            obj = cls(**constructor_kwargs)
            obj.faulty_service = spec["faulty_service"]
        elif mode == "fixed":
            obj = cls(**constructor_kwargs)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

        # Apply post-construction overrides for variants the constructor can't handle.
        if variant_kwargs and not constructor_kwargs:
            _apply_variant_override(obj, variant_kwargs, spec)

        return obj

    return factory


# ==========================================================
# SPEC GENERATION
# ==========================================================

def generate_single_fault_specs():
    count = 0

    for fault_name, cfg in FAULT_FAMILIES.items():
        mode = cfg["mode"]

        if mode in ["constructor_service", "override_service"]:
            for task_name, task_cfg in cfg["tasks"].items():
                variants = get_supported_variants_for_task(task_cfg, fault_name)

                for service in cfg["services"]:
                    for variant in variants:
                        variant_name = variant["variant"]

                        problem_id = (
                            f"gen_{fault_name}-{task_name}-{service}-{variant_name}"
                        )

                        spec = {
                            "problem_id": problem_id,
                            "fault_family": fault_name,
                            "task": task_name,
                            "faulty_service": service,
                            "mode": mode,
                            "deployment": cfg.get("deployment", "k8s"),
                            "folder_name": task_cfg["folder_name"],
                            "py_file_name": task_cfg["py_file_name"],
                            "class_name": task_cfg["class_name"],
                            "variant": variant,
                            "app": cfg.get("app", "unknown"),
                            "is_multifault": False,
                        }

                        yield spec
                        count += 1
                        if count >= MAX_GENERATED:
                            return

        elif mode == "constructor_app":
            for task_name, task_cfg in cfg["tasks"].items():
                variants = get_supported_variants_for_task(task_cfg, fault_name)

                for app_name in cfg["apps"]:
                    for variant in variants:
                        variant_name = variant["variant"]

                        problem_id = (
                            f"gen_{fault_name}-{task_name}-{app_name}-{variant_name}"
                        )

                        spec = {
                            "problem_id": problem_id,
                            "fault_family": fault_name,
                            "task": task_name,
                            "app_name": app_name,
                            "app": cfg.get("app", "unknown"),
                            "mode": mode,
                            "deployment": cfg.get("deployment", "k8s"),
                            "folder_name": task_cfg["folder_name"],
                            "py_file_name": task_cfg["py_file_name"],
                            "class_name": task_cfg["class_name"],
                            "variant": variant,
                            "is_multifault": False,
                        }

                        yield spec
                        count += 1
                        if count >= MAX_GENERATED:
                            return

        elif mode == "fixed":
            for task_name, task_cfg in cfg["tasks"].items():
                variants = get_supported_variants_for_task(task_cfg, fault_name)

                for variant in variants:
                    variant_name = variant["variant"]

                    problem_id = f"gen_{fault_name}-{task_name}-{variant_name}"

                    spec = {
                        "problem_id": problem_id,
                        "fault_family": fault_name,
                        "task": task_name,
                        "app": cfg.get("app", "unknown"),
                        "mode": mode,
                        "deployment": cfg.get("deployment", "k8s"),
                        "folder_name": task_cfg["folder_name"],
                        "py_file_name": task_cfg["py_file_name"],
                        "class_name": task_cfg["class_name"],
                        "variant": variant,
                        "is_multifault": False,
                    }

                    yield spec
                    count += 1
                    if count >= MAX_GENERATED:
                        return

        else:
            raise ValueError(f"Unsupported generation mode: {mode}")


def generate_multifault_specs(single_specs, max_pairs=500):
    """
    Generates multi-fault specs by pairing compatible single-fault specs.

    We only pair:
      - same deployment type
      - same task when possible
      - non-noop faults
      - not already multifault
    """

    candidates = [
        s for s in single_specs
        if not s.get("is_multifault")
        and s.get("fault_family") != "noop_detection"
        and s.get("deployment", "k8s") == "k8s"
    ]

    produced = 0

    for a, b in itertools.combinations(candidates, 2):
        if produced >= max_pairs:
            return

        if a.get("task") != b.get("task"):
            continue

        if a.get("app") != b.get("app"):
            continue

        if (
            a.get("fault_family") in READINESS_BREAKING_FAULTS
            or b.get("fault_family") in READINESS_BREAKING_FAULTS
        ):
            continue

        if (
            a.get("fault_family") == b.get("fault_family")
            and a.get("faulty_service") == b.get("faulty_service")
        ):
            continue

        # Avoid huge duplicate behavior from different variants of same class.
        if (
            a.get("folder_name") == b.get("folder_name")
            and a.get("py_file_name") == b.get("py_file_name")
            and a.get("class_name") == b.get("class_name")
        ):
            continue

        problem_id = (
            "gen_multifault__"
            f"{a['fault_family']}__{a.get('faulty_service', a.get('app_name', 'fixed'))}"
            "__PLUS__"
            f"{b['fault_family']}__{b.get('faulty_service', b.get('app_name', 'fixed'))}"
            f"__{a.get('task')}"
        )

        yield {
            "problem_id": problem_id,
            "fault_family": "multifault",
            "task": a.get("task"),
            "mode": "multifault",
            "deployment": "k8s",
            "is_multifault": True,
            "subproblems": [a, b],
        }

        produced += 1


def generate_specs():
    only_multi = os.environ.get("AIOPSLAB_ONLY_MULTIFAULT", "0").strip() == "1"
    max_multi  = int(os.environ.get("AIOPSLAB_MAX_MULTIFAULT", "1000"))

    single_specs = list(generate_single_fault_specs())

    if not only_multi:
        for s in single_specs:
            yield s

    for s in generate_multifault_specs(single_specs, max_pairs=max_multi):
        yield s

class MultiFaultProblem:
    """
    Generic wrapper for two AIOpsLab problems.

    It delegates most fields to the first problem, but tries to call
    lifecycle/fault methods on both.

    This is intentionally defensive because different AIOpsLab problem
    classes may expose different method names.
    """

    MULTI_CALL_METHODS = [
        "setup",
        "inject_fault",
        "recover",
        "cleanup",
        "teardown",
        "reset",
    ]

    def __init__(self, subproblem_factories):
        self.subproblems = [f() for f in subproblem_factories]
        self.primary = self.subproblems[0]

        self.problem_id = getattr(self.primary, "problem_id", "multifault")
        self.faulty_service = ",".join(
            str(getattr(p, "faulty_service", "unknown"))
            for p in self.subproblems
        )

    def __getattr__(self, name):
        if name in self.MULTI_CALL_METHODS:
            return self._make_multi_method(name)

        return getattr(self.primary, name)

    def _make_multi_method(self, name):
        async def async_multi(*args, **kwargs):
            results = []

            for p in self.subproblems:
                fn = getattr(p, name, None)
                if fn is None:
                    continue

                result = fn(*args, **kwargs)

                if inspect.isawaitable(result):
                    result = await result

                results.append(result)

            return results[-1] if results else None

        def sync_multi(*args, **kwargs):
            results = []

            for p in self.subproblems:
                fn = getattr(p, name, None)
                if fn is None:
                    continue

                result = fn(*args, **kwargs)
                results.append(result)

            return results[-1] if results else None

        # If any method is async, expose async wrapper.
        for p in self.subproblems:
            fn = getattr(p, name, None)
            if fn is not None and inspect.iscoroutinefunction(fn):
                return async_multi

        return sync_multi

def make_multifault_problem_factory(spec: Dict[str, Any]) -> Callable:
    sub_specs = spec["subproblems"]

    sub_factories = [
        make_problem_factory(s)
        for s in sub_specs
    ]

    def factory():
        return MultiFaultProblem(sub_factories)

    return factory


def register_generated_problem(orchestrator: Orchestrator, spec: Dict[str, Any]):
    if spec.get("mode") == "multifault":
        orchestrator.probs.PROBLEM_REGISTRY[spec["problem_id"]] = (
            make_multifault_problem_factory(spec)
        )
    else:
        orchestrator.probs.PROBLEM_REGISTRY[spec["problem_id"]] = (
            make_problem_factory(spec)
        )

    if spec.get("deployment") == "docker":
        if spec["problem_id"] not in orchestrator.probs.DOCKER_REGISTRY:
            orchestrator.probs.DOCKER_REGISTRY.append(spec["problem_id"])


# ==========================================================
# RUN ONE PROBLEM
# ==========================================================

def save_builtin_api_outputs(records: List[Dict[str, Any]], out_dir: Path):
    api_dir = out_dir / "builtin_api_outputs"
    logs_dir = api_dir / "logs"
    metrics_dir = api_dir / "metrics"
    traces_dir = api_dir / "traces"
    shell_dir = api_dir / "shell"
    repo_dir = api_dir / "repo_diff"

    for d in [api_dir, logs_dir, metrics_dir, traces_dir, shell_dir, repo_dir]:
        d.mkdir(parents=True, exist_ok=True)

    for rec in records:
        step = rec["step"]
        action = rec["action"]
        output = rec.get("output") or ""

        action_lower = action.lower()

        if action_lower.startswith("get_logs"):
            write_text(logs_dir / f"step_{step:03d}_get_logs.txt", output)

        elif action_lower.startswith("get_metrics"):
            write_text(metrics_dir / f"step_{step:03d}_get_metrics.txt", output)

        elif action_lower.startswith("get_traces"):
            write_text(traces_dir / f"step_{step:03d}_get_traces.txt", output)

        elif action_lower.startswith("get_microservice_repo_diff"):
            write_text(repo_dir / f"step_{step:03d}_repo_diff.txt", output)

        elif action_lower.startswith("exec_shell"):
            write_text(shell_dir / f"step_{step:03d}_shell_output.txt", output)

    write_json(api_dir / "all_records.json", records)


def collect_direct_k8s(namespace: str, out_dir: Path):
    k8s_dir = out_dir / "direct_k8s_outputs"
    pod_logs_dir = k8s_dir / "pod_logs"

    k8s_dir.mkdir(parents=True, exist_ok=True)
    pod_logs_dir.mkdir(parents=True, exist_ok=True)

    commands = {
        "pods.json": f"kubectl get pods -n {namespace} -o json",
        "pods_wide.txt": f"kubectl get pods -n {namespace} -o wide",
        "services.json": f"kubectl get svc -n {namespace} -o json",
        "services_wide.txt": f"kubectl get svc -n {namespace} -o wide",
        "deployments.json": f"kubectl get deploy -n {namespace} -o json",
        "deployments_wide.txt": f"kubectl get deploy -n {namespace} -o wide",
        "replicasets.json": f"kubectl get rs -n {namespace} -o json",
        "endpoints.json": f"kubectl get endpoints -n {namespace} -o json",
        "events.txt": f"kubectl get events -n {namespace} --sort-by=.lastTimestamp",
        "describe_pods.txt": f"kubectl describe pods -n {namespace}",
        "top_pods.txt": f"kubectl top pods -n {namespace} || true",
        "top_nodes.txt": "kubectl top nodes || true",
    }

    status = {}

    for fname, cmd in commands.items():
        res = run_cmd(cmd)
        status[fname] = {
            "cmd": cmd,
            "returncode": res["returncode"],
            "stderr": res["stderr"],
        }
        write_text(k8s_dir / fname, res["stdout"])
        write_text(k8s_dir / f"{fname}.stderr", res["stderr"])

    pod_names = run_cmd(
        f"kubectl get pods -n {namespace} -o jsonpath='{{.items[*].metadata.name}}'"
    )
    pods = pod_names["stdout"].strip().replace("'", "").split()

    for pod in pods:
        res = run_cmd(
            f"kubectl logs -n {namespace} {pod} --all-containers --prefix --tail=1500",
            timeout=180,
        )
        write_text(pod_logs_dir / f"{safe_name(pod)}.log", res["stdout"])
        write_text(pod_logs_dir / f"{safe_name(pod)}.stderr", res["stderr"])

    write_json(k8s_dir / "collection_status.json", status)


def validate_unready_pods(namespace, spec, scenario_dir):
    expected = get_expected_faulty_services(spec)

    pods_json_path = scenario_dir / "direct_k8s_outputs" / "pods.json"
    pods = load_kubectl_json(pods_json_path)

    unexpected_unready = []

    for pod in pods.get("items", []):
        pod_name = pod.get("metadata", {}).get("name", "")
        labels = pod.get("metadata", {}).get("labels", {})
        statuses = pod.get("status", {}).get("containerStatuses", []) or []

        ready = bool(statuses) and all(s.get("ready", False) for s in statuses)

        if ready:
            continue

        app_label = (
            labels.get("app")
            or labels.get("app.kubernetes.io/name")
            or labels.get("service")
            or labels.get("io.kompose.service")
            or ""
        )

        matched_expected = any(
            svc in pod_name or svc == app_label
            for svc in expected
        )

        if not matched_expected:
            unexpected_unready.append(
                {
                    "pod": pod_name,
                    "app_label": app_label,
                    "phase": pod.get("status", {}).get("phase"),
                    "expected_faulty_services": sorted(expected),
                }
            )

    write_json(
        scenario_dir / "validation_unready_pods.json",
        {
            "expected_faulty_services": sorted(expected),
            "unexpected_unready": unexpected_unready,
            "ok": len(unexpected_unready) == 0,
        },
    )

    if unexpected_unready:
        # Do NOT fail the scenario here.  Faults like revoke_auth_mongodb cause
        # *cascade* effects: e.g. revoking mongodb-geo auth makes the geo service
        # pod crash — geo is not in expected_faulty_services but its failure is
        # the correct observable symptom of the injected fault.  Raising here
        # would incorrectly discard valid telemetry.
        # The details are already written to validation_unready_pods.json for
        # post-hoc analysis.
        print(
            f"[WARN] validate_unready_pods: {len(unexpected_unready)} pod(s) unready "
            f"outside expected_faulty_services {sorted(expected)!r} — likely cascade "
            f"effect of the injected fault. Continuing. "
            f"(see validation_unready_pods.json)"
        )


def load_kubectl_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def build_topology_and_graph(namespace: str, out_dir: Path):
    direct_dir = out_dir / "direct_k8s_outputs"
    topo_dir = out_dir / "topology"
    topo_dir.mkdir(parents=True, exist_ok=True)

    services_json = load_kubectl_json(direct_dir / "services.json")
    pods_json = load_kubectl_json(direct_dir / "pods.json")
    endpoints_json = load_kubectl_json(direct_dir / "endpoints.json")

    topology = {
        "namespace": namespace,
        "services": [],
        "pods": [],
        "edges": [],
    }

    for svc in services_json.get("items", []):
        topology["services"].append(
            {
                "name": svc.get("metadata", {}).get("name"),
                "selector": svc.get("spec", {}).get("selector", {}),
                "ports": svc.get("spec", {}).get("ports", []),
                "cluster_ip": svc.get("spec", {}).get("clusterIP"),
            }
        )

    for pod in pods_json.get("items", []):
        topology["pods"].append(
            {
                "name": pod.get("metadata", {}).get("name"),
                "labels": pod.get("metadata", {}).get("labels", {}),
                "phase": pod.get("status", {}).get("phase"),
                "node": pod.get("spec", {}).get("nodeName"),
                "pod_ip": pod.get("status", {}).get("podIP"),
            }
        )

    for ep in endpoints_json.get("items", []):
        svc_name = ep.get("metadata", {}).get("name")
        for subset in ep.get("subsets", []) or []:
            for addr in subset.get("addresses", []) or []:
                target = addr.get("targetRef", {}).get("name", addr.get("ip"))
                topology["edges"].append(
                    {
                        "from_service": svc_name,
                        "to_pod_or_ip": target,
                    }
                )

    graph = {
        "nodes": (
            [{"id": s["name"], "type": "service", **s} for s in topology["services"]]
            + [{"id": p["name"], "type": "pod", **p} for p in topology["pods"]]
        ),
        "edges": topology["edges"],
    }

    write_json(topo_dir / "topology.json", topology)
    write_json(topo_dir / "graph.json", graph)


def build_sla(namespace: str, out_dir: Path):
    direct_dir = out_dir / "direct_k8s_outputs"
    sla_dir = out_dir / "sla"
    sla_dir.mkdir(parents=True, exist_ok=True)

    pods_json = load_kubectl_json(direct_dir / "pods.json")
    events_text = (
        (direct_dir / "events.txt").read_text(errors="replace")
        if (direct_dir / "events.txt").exists()
        else ""
    )

    sla = {
        "namespace": namespace,
        "total_pods": 0,
        "ready_pods": 0,
        "not_ready_pods": [],
        "restart_count_total": 0,
        "warning_events_present": "Warning" in events_text,
        "ready_ratio": 0.0,
    }

    for pod in pods_json.get("items", []):
        pod_name = pod.get("metadata", {}).get("name")
        statuses = pod.get("status", {}).get("containerStatuses", []) or []
        ready = bool(statuses) and all(s.get("ready", False) for s in statuses)
        restarts = sum(int(s.get("restartCount", 0)) for s in statuses)

        sla["total_pods"] += 1
        sla["restart_count_total"] += restarts

        if ready:
            sla["ready_pods"] += 1
        else:
            sla["not_ready_pods"].append(pod_name)

    if sla["total_pods"]:
        sla["ready_ratio"] = sla["ready_pods"] / sla["total_pods"]

    write_json(sla_dir / "sla_results.json", sla)
    write_text(sla_dir / "events.txt", events_text)


def build_twin_files(spec: Dict[str, Any], namespace: str, apis: Dict[str, Any], out_dir: Path):
    twin_dir = out_dir / "twin_inputs"
    twin_dir.mkdir(parents=True, exist_ok=True)

    api_names = list(apis.keys())

    action_library = {
        "available_actions": api_names,
        "api_docs": {k: str(v) for k, v in apis.items()},
        "builtin_expected_actions": [
            "get_logs",
            "get_metrics",
            "get_traces",
            "exec_shell",
            "submit",
            "get_microservice_repo_diff",
        ],
    }

    fault_library = {
        "problem_id": spec.get("problem_id"),
        "fault_family": spec.get("fault_family"),
        "task": spec.get("task"),
        "faulty_service": spec.get("faulty_service"),
        "app_name": spec.get("app_name"),
        "mode": spec.get("mode"),
        "deployment": spec.get("deployment"),
        "folder_name": spec.get("folder_name"),
        "py_file_name": spec.get("py_file_name"),
        "class_name": spec.get("class_name"),
        "variant": spec.get("variant"),
        "is_multifault": spec.get("is_multifault"),
        "subproblems": spec.get("subproblems"),
    }

    observation_model = {
        "namespace": namespace,
        "sources": {
            "builtin_logs": "builtin_api_outputs/logs",
            "builtin_metrics": "builtin_api_outputs/metrics",
            "builtin_traces": "builtin_api_outputs/traces",
            "builtin_shell": "builtin_api_outputs/shell",
            "direct_k8s": "direct_k8s_outputs",
            "pod_logs": "direct_k8s_outputs/pod_logs",
            "topology": "topology/topology.json",
            "graph": "topology/graph.json",
            "sla": "sla/sla_results.json",
        },
        "available_api_groups": {
            "logs": [a for a in api_names if "log" in a.lower()],
            "metrics": [a for a in api_names if "metric" in a.lower()],
            "traces": [a for a in api_names if "trace" in a.lower()],
            "shell": [a for a in api_names if "shell" in a.lower()],
            "repo_diff": [
                a for a in api_names
                if "repo" in a.lower() or "diff" in a.lower()
            ],
            "submit": [a for a in api_names if "submit" in a.lower()],
        },
    }

    twin_spec = {
        "problem_id": spec.get("problem_id"),
        "namespace": namespace,
        "scenario_spec": spec,
        "fault_library_file": "fault_library.json",
        "action_library_file": "action_library.json",
        "observation_model_file": "observation_model.json",
        "topology_file": "../topology/topology.json",
        "graph_file": "../topology/graph.json",
        "sla_file": "../sla/sla_results.json",
    }

    complexity_report = {
        "num_available_actions": len(api_names),
        "has_logs": "get_logs" in api_names,
        "has_metrics": "get_metrics" in api_names,
        "has_traces": "get_traces" in api_names,
        "has_shell": "exec_shell" in api_names,
        "has_repo_diff": "get_microservice_repo_diff" in api_names,
        "deployment": spec.get("deployment", "k8s"),
        "mode": spec.get("mode"),
        "task": spec.get("task"),
        "is_multifault": spec.get("is_multifault", False),
    }

    write_json(twin_dir / "action_library.json", action_library)
    write_json(twin_dir / "fault_library.json", fault_library)
    write_json(twin_dir / "observation_model.json", observation_model)
    write_json(twin_dir / "twin_spec.json", twin_spec)
    write_json(twin_dir / "complexity_report.json", complexity_report)


# ==========================================================
# GROUND TRUTH LOOKUP
# Maps fault_family -> (system_level, fault_type) for analysis tasks.
# Values mirror the expected answers inside each problem class's eval().
# ==========================================================

ANALYSIS_GROUND_TRUTH: Dict[str, Dict[str, str]] = {
    "k8s_target_port_misconfig":            {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
    "auth_miss_mongodb":                    {"system_level": "Application", "fault_type": "Misconfiguration"},
    "scale_pod_zero_social_net":            {"system_level": "Kubernetes", "fault_type": "Resource"},
    "assign_to_non_existent_node_social_net": {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
    "revoke_auth_mongodb":                  {"system_level": "Application", "fault_type": "Misconfiguration"},
    "user_unregistered_mongodb":            {"system_level": "Application", "fault_type": "Misconfiguration"},
    "misconfig_app_hotel_res":              {"system_level": "Application", "fault_type": "Misconfiguration"},
    "wrong_bin_usage":                      {"system_level": "Application", "fault_type": "Misconfiguration"},
    "redeploy_without_pv":                  {"system_level": "Kubernetes", "fault_type": "Resource"},
    "network_loss_hotel_res":               {"system_level": "Network",     "fault_type": "Network Loss"},
    "network_delay_hotel_res":              {"system_level": "Network",     "fault_type": "Network Delay"},
    "pod_failure_hotel_res":                {"system_level": "Kubernetes", "fault_type": "Pod Failure"},
    "pod_kill_hotel_res":                   {"system_level": "Kubernetes", "fault_type": "Pod Kill"},
    "container_kill_hotel_res":             {"system_level": "Kubernetes", "fault_type": "Container Kill"},
    "astronomy_shop_ad_service_failure":    {"system_level": "Application", "fault_type": "Service Failure"},
    "astronomy_shop_ad_service_high_cpu":   {"system_level": "Application", "fault_type": "Resource Exhaustion"},
    "astronomy_shop_ad_service_manual_gc":  {"system_level": "Application", "fault_type": "Misconfiguration"},
    "astronomy_shop_cart_service_failure":  {"system_level": "Application", "fault_type": "Service Failure"},
    "astronomy_shop_image_slow_load":       {"system_level": "Application", "fault_type": "Performance Degradation"},
    "astronomy_shop_kafka_queue_problems":  {"system_level": "Application", "fault_type": "Queue Overflow"},
    "astronomy_shop_loadgenerator_flood_homepage": {"system_level": "Network", "fault_type": "Traffic Flood"},
    "astronomy_shop_payment_service_failure":      {"system_level": "Application", "fault_type": "Service Failure"},
    "astronomy_shop_payment_service_unreachable":  {"system_level": "Network",     "fault_type": "Service Unreachable"},
    "astronomy_shop_product_catalog_service_failure": {"system_level": "Application", "fault_type": "Service Failure"},
    "astronomy_shop_recommendation_service_cache_failure": {"system_level": "Application", "fault_type": "Cache Failure"},
    "flower_node_stop":                     {"system_level": "Application", "fault_type": "Node Failure"},
    "flower_model_misconfig":               {"system_level": "Application", "fault_type": "Misconfiguration"},
    "operator_non_existent_storage":        {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
    "operator_overload_replicas":           {"system_level": "Kubernetes", "fault_type": "Resource"},
    "operator_security_context_fault":      {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
    "operator_wrong_update_strategy":       {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
    "operator_invalid_affinity_toleration": {"system_level": "Kubernetes", "fault_type": "Misconfiguration"},
}


def save_ground_truth(problem: Any, spec: Dict[str, Any], out_dir: Path):
    """
    Save the correct answer for this scenario to ground_truth.json.

    For multifault specs, answers are aggregated across subproblems.
    """
    task = spec.get("task", "")
    fault_family = spec.get("fault_family", "")
    is_multifault = spec.get("is_multifault", False)

    def _answer_for(prob, sub_spec):
        t = sub_spec.get("task", task)
        ff = sub_spec.get("fault_family", fault_family)
        svc = getattr(prob, "faulty_service", sub_spec.get("faulty_service"))

        if t == "detection":
            return "Yes"
        if t == "localization":
            return [svc] if svc else []
        if t == "analysis":
            gt = ANALYSIS_GROUND_TRUTH.get(ff, {})
            return {
                "system_level": gt.get("system_level", "unknown"),
                "fault_type":   gt.get("fault_type",   "unknown"),
            }
        if t == "mitigation":
            return getattr(prob, "expected_mitigation", "unknown")
        return "unknown"

    if is_multifault and hasattr(problem, "subproblems"):
        sub_answers = []
        for i, sub_prob in enumerate(problem.subproblems):
            sub_spec = spec["subproblems"][i] if i < len(spec.get("subproblems", [])) else {}
            sub_answers.append({
                "fault_family":   sub_spec.get("fault_family"),
                "faulty_service": getattr(sub_prob, "faulty_service", None),
                "answer":         _answer_for(sub_prob, sub_spec),
            })
        gt = {
            "task":          task,
            "is_multifault": True,
            "subproblem_answers": sub_answers,
            # For localization/detection, the combined answer is the union.
            "answer": (
                [a["faulty_service"] for a in sub_answers if a["faulty_service"]]
                if task in ("localization", "detection") else sub_answers
            ),
        }
    else:
        gt = {
            "task":           task,
            "fault_family":   fault_family,
            "faulty_service": getattr(problem, "faulty_service", spec.get("faulty_service")),
            "app":            spec.get("app"),
            "variant":        spec.get("variant", {}).get("variant", "default"),
            "is_multifault":  False,
            "answer":         _answer_for(problem, spec),
        }

    # Append analysis lookup values for easy reference even on non-analysis tasks.
    if fault_family in ANALYSIS_GROUND_TRUTH:
        gt["analysis_ground_truth"] = ANALYSIS_GROUND_TRUTH[fault_family]

    write_json(out_dir / "ground_truth.json", gt)


def save_fault_timing(out_dir: Path, inject_time: str, collect_time: str):
    write_json(out_dir / "fault_timing.json", {
        "fault_injected_at":              inject_time,
        "telemetry_collected_at":         collect_time,
        "wait_after_fault_sec":           WAIT_AFTER_FAULT_SEC,
        "approx_symptom_window_start":    inject_time,
        "approx_symptom_window_end":      collect_time,
    })


def write_master_index(all_specs: List[Dict[str, Any]], out_dir: Path):
    """Write dataset_index.json and dataset_index.csv covering all specs."""
    rows = []
    for spec in all_specs:
        scenario_dir = TELEMETRY_DIR / safe_name(spec["problem_id"])
        rows.append({
            "problem_id":     spec["problem_id"],
            "app":            spec.get("app", "unknown"),
            "fault_family":   spec.get("fault_family"),
            "task":           spec.get("task"),
            "faulty_service": spec.get("faulty_service", ""),
            "variant":        spec.get("variant", {}).get("variant", "default"),
            "is_multifault":  spec.get("is_multifault", False),
            "deployment":     spec.get("deployment", "k8s"),
            "mode":           spec.get("mode"),
            "completed":      (scenario_dir / "DONE.json").exists(),
            "passed":         (PASSED_DIR / f"{safe_name(spec['problem_id'])}.json").exists(),
            "failed":         (FAILED_DIR / f"{safe_name(spec['problem_id'])}.json").exists(),
            "ground_truth_path": str(scenario_dir / "ground_truth.json"),
            "telemetry_path": str(scenario_dir),
        })

    write_json(out_dir / "dataset_index.json", rows)

    if rows:
        with open(out_dir / "dataset_index.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"[INDEX] Wrote dataset index: {len(rows)} scenarios → {out_dir / 'dataset_index.json'}")


def _cleanup_pvcs(namespace: str) -> None:
    """Delete all PVCs in *namespace* so the next scenario starts with clean storage.

    Faults such as revoke_auth_mongodb write broken credentials into MongoDB's
    data directory, which is stored in a PersistentVolume.  If the PVC survives
    the app redeploy, every subsequent scenario that shares the same namespace
    inherits the broken auth state and enters CrashLoopBackOff before any fault
    is even injected.  Deleting the PVCs forces Kubernetes to create fresh
    volumes on the next deploy.

    Only namespaces that start with 'test-' are touched (AIOpsLab sandbox
    namespaces); system namespaces are never affected.
    """
    if not namespace or not namespace.startswith("test-"):
        return
    result = run_cmd(
        f"kubectl --context kind-kind delete pvc --all -n {namespace} "
        f"--ignore-not-found",
        timeout=90,
    )
    if result.get("returncode") == 0:
        deleted = result.get("stdout", "").strip()
        print(f"[CLEANUP] PVCs removed from '{namespace}': "
              f"{deleted or 'none found'}")
    else:
        print(f"[CLEANUP] PVC cleanup warning for '{namespace}': "
              f"{result.get('stderr','')[:200]}")


async def run_one(spec: Dict[str, Any]):
    async def _run():
        scenario_dir = TELEMETRY_DIR / safe_name(spec["problem_id"])
        scenario_dir.mkdir(parents=True, exist_ok=True)

        orchestrator = Orchestrator(
            results_dir=scenario_dir / "aiopslab_session_results"
        )

        agent = BuiltinFullCollectorAgent(
            task=spec.get("task", ""),
            faulty_service=spec.get("faulty_service"),
        )

        orchestrator.register_agent(agent, name="builtin_full_collector")
        register_generated_problem(orchestrator, spec)

        # init_problem deploys the app AND injects the fault.
        fault_injected_at = datetime.now().isoformat()
        problem_desc, instructions, apis = orchestrator.init_problem(spec["problem_id"])

        problem = orchestrator.session.problem

        # Fix missing namespace for some AIOpsLab problem classes.
        namespace = getattr(problem, "namespace", None)

        if namespace is None:
            app = getattr(problem, "app", None)

            if app is not None:
                namespace = getattr(app, "namespace", None)

            if namespace is None:
                if spec.get("deployment") == "docker":
                    namespace = "docker"
                else:
                    namespace = "default"

            # orchestrator.start_problem() later directly accesses
            # self.session.problem.namespace
            problem.namespace = namespace

        agent.namespace = namespace
        agent.configure_actions(apis)

        write_json(scenario_dir / "spec.json", spec)
        write_text(scenario_dir / "problem_desc.txt", str(problem_desc))
        write_text(scenario_dir / "instructions.txt", str(instructions))
        write_json(
            scenario_dir / "available_apis.json",
            {k: str(v) for k, v in apis.items()},
        )
        write_json(scenario_dir / "api_action_plan.json", agent.actions)

        # --- Ground truth (saved before any cleanup so problem attrs are intact) ---
        print("[GROUND TRUTH] Saving expected answers...")
        save_ground_truth(problem, spec, scenario_dir)

        print(f"[WAIT] Fault injected. Waiting {WAIT_AFTER_FAULT_SEC}s...")
        time.sleep(WAIT_AFTER_FAULT_SEC)
        telemetry_collected_at = datetime.now().isoformat()

        # --- Fault timing ---
        save_fault_timing(scenario_dir, fault_injected_at, telemetry_collected_at)

        print("[COLLECT] Direct Kubernetes telemetry before submit/cleanup...")
        collect_direct_k8s(namespace, scenario_dir)
        validate_unready_pods(namespace, spec, scenario_dir)

        print("[BUILD] topology, graph, SLA, and twin files...")
        build_topology_and_graph(namespace, scenario_dir)
        build_sla(namespace, scenario_dir)
        build_twin_files(spec, namespace, apis, scenario_dir)

        print("[RUN] Built-in telemetry API calls...")
        run_result = await orchestrator.start_problem(
            max_steps=len(agent.actions)
        )

        write_json(scenario_dir / "agent_transcript.json", agent.records)
        write_json(
            scenario_dir / "run_result.json",
            {
                "raw_result_str": str(run_result),
                "type": type(run_result).__name__,
            },
        )

        print("[ORGANIZE] Built-in API outputs...")
        save_builtin_api_outputs(agent.records, scenario_dir)

        done = {
            "ok": True,
            "reason": "completed_with_telemetry",
            "problem_id": spec["problem_id"],
            "namespace": namespace,
            "scenario_dir": str(scenario_dir),
            "fault_injected_at": fault_injected_at,
            "telemetry_collected_at": telemetry_collected_at,
            "end_time": datetime.now().isoformat(),
        }
        write_json(scenario_dir / "DONE.json", done)

        return {
            "ok": True,
            "reason": "initialized_collected_and_started",
            "problem_id": spec["problem_id"],
            "spec": spec,
            "namespace": namespace,
            "scenario_dir": str(scenario_dir),
            "problem_desc": str(problem_desc)[:1000],
            "apis": list(apis.keys()),
        }

    try:
        return await asyncio.wait_for(_run(), timeout=TIMEOUT_SEC)

    except Exception as e:
        return {
            "ok": False,
            "reason": str(e),
            "problem_id": spec["problem_id"],
            "spec": spec,
            "traceback": traceback.format_exc(),
        }



# ==========================================================
# MAIN
# ==========================================================

def _prewarm_social_network():
    """
    Deploy social-network once and wait for all pods to be Ready.
    This pre-pulls all container images into the local Docker/kind cache so
    every subsequent scenario starts fast.  Uses a long timeout (30 min)
    because the first pull of deathstarbench images can take a while on a
    slow connection.
    """
    from aiopslab.service.helm import Helm
    from aiopslab.service.kubectl import KubeCtl
    from aiopslab.service.apps.socialnet import SocialNetwork

    PREWARM_TIMEOUT = int(os.environ.get("AIOPSLAB_PREWARM_TIMEOUT", 1800))

    print("\n[PRE-WARM] Deploying social-network to cache images …")
    # Temporarily raise the pod-ready timeout so assert_if_deployed (called
    # inside app.deploy()) uses the longer prewarm timeout, not 900 s.
    _orig = os.environ.get("AIOPSLAB_POD_READY_TIMEOUT")
    os.environ["AIOPSLAB_POD_READY_TIMEOUT"] = str(PREWARM_TIMEOUT)
    try:
        app = SocialNetwork()
        # Uninstall any leftover release first
        try:
            Helm.uninstall(release_name="social-network", namespace=app.namespace)
        except Exception:
            pass

        app.deploy()   # internally calls wait_for_ready with PREWARM_TIMEOUT
        print("[PRE-WARM] All pods ready — images are cached.")

        # Leave it deployed so the first scenario can reuse it (helm uninstall
        # happens at the start of each scenario anyway).
    except Exception as e:
        print(f"[PRE-WARM] Warning: pre-warm failed ({e}). "
              "Images may not be cached; first scenario could be slow.")
    finally:
        # Restore the original per-scenario timeout
        if _orig is None:
            os.environ.pop("AIOPSLAB_POD_READY_TIMEOUT", None)
        else:
            os.environ["AIOPSLAB_POD_READY_TIMEOUT"] = _orig


async def main():
    mkdirs()

    # ── Pre-warm: pull all images into the local cache once ──────────────────
    _prewarm_social_network()

    all_specs = list(generate_specs())

    # Shard the scenario list when running multiple parallel workers.
    # Striped slicing (not chunked) gives each worker a balanced mix of
    # apps/fault types rather than one worker doing all social-network.
    if _NUM_WORKERS > 1:
        all_specs = all_specs[_WORKER_ID::_NUM_WORKERS]

    runnable_specs = []
    skipped = 0

    for spec in all_specs:
        if result_path_exists(spec["problem_id"]):
            skipped += 1
            continue
        runnable_specs.append(spec)

    print("====================================")
    print("Dynamic AIOpsLab Scenario Generator")
    print("====================================")
    if _NUM_WORKERS > 1:
        print(f"Worker:                {_WORKER_ID + 1} / {_NUM_WORKERS}")
    print(f"Total candidate specs: {len(all_specs)}")
    print(f"Skipped existing:      {skipped}")
    print(f"To run now:            {len(runnable_specs)}")
    print(f"Batch size:            {BATCH_SIZE}")
    print(f"Max steps per problem: {MAX_STEPS}")
    print(f"Timeout per problem:   {TIMEOUT_SEC}s")
    print("====================================")

    passed = len(list(PASSED_DIR.glob("*.json")))
    failed = len(list(FAILED_DIR.glob("*.json")))

    for i in range(0, len(runnable_specs), BATCH_SIZE):
        batch = runnable_specs[i:i + BATCH_SIZE]

        print(
            f"\nTesting batch {i // BATCH_SIZE + 1} / "
            f"{(len(runnable_specs) + BATCH_SIZE - 1) // BATCH_SIZE}"
        )

        for spec in batch:
            pid = spec["problem_id"]
            print(f"\n[RUN] {pid}")

            spec_path = SPECS_DIR / f"{safe_name(pid)}.json"

            # Write spec only when we actually run it.
            write_json(spec_path, spec)

            result = await run_one(spec)

            # ── PVC cleanup ──────────────────────────────────────────────────
            # Must happen after EVERY scenario (pass or fail).  Faults like
            # revoke_auth_mongodb corrupt MongoDB's on-disk auth data inside a
            # PVC.  Without this, each new hotel-reservation deploy inherits the
            # broken auth from the previous scenario → cascading crashes.
            cleanup_ns = result.get("namespace")
            if not cleanup_ns:
                # Failure path: _run() raised before namespace was captured;
                # derive it from the problem_id.
                _pid = spec.get("problem_id", "")
                if "hotel_res" in _pid:
                    cleanup_ns = "test-hotel-reservation"
                elif "social" in _pid or "socialnet" in _pid:
                    cleanup_ns = "social-network"
            _cleanup_pvcs(cleanup_ns or "")
            # ────────────────────────────────────────────────────────────────

            if result["ok"]:
                passed += 1
                print(f"[PASS] {pid}")
                write_json(PASSED_DIR / f"{safe_name(pid)}.json", result)
            else:
                failed += 1
                print(f"[FAIL] {pid}")
                print("Reason:", result["reason"])
                write_json(FAILED_DIR / f"{safe_name(pid)}.json", result)

            append_log(result)

            print(f"Current passed={passed}, failed={failed}")

    # Write master dataset index covering all specs (completed or not).
    write_master_index(all_specs, OUT_DIR)

    print("\n====================================")
    print("DONE")
    print("====================================")
    print(f"Existing/skipped: {skipped}")
    print(f"Passed total:     {len(list(PASSED_DIR.glob('*.json')))}")
    print(f"Failed total:     {len(list(FAILED_DIR.glob('*.json')))}")
    print(f"Specs total:      {len(list(SPECS_DIR.glob('*.json')))}")
    print(f"Specs:            {SPECS_DIR}")
    print(f"Good:             {PASSED_DIR}")
    print(f"Telemetry:        {TELEMETRY_DIR}")
    print(f"Bad:              {FAILED_DIR}")
    print(f"Log:              {LOG_FILE}")
    print(f"Dataset index:    {OUT_DIR / 'dataset_index.json'}")


if __name__ == "__main__":
    asyncio.run(main())