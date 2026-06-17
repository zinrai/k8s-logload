#!/usr/bin/env python3
"""k8s-logload -- generate a Kubernetes log workload for stressing a log pipeline.

Creates log-emitting Pods (`pod-up`/`pod-down`) or a Tekton TaskRun (`task-run`)
via kubectl, and emits JSON pod records on stdout so a measurer (e.g.
k8s-loki-logbench) can consume them by pipe. Log-backend agnostic; stdlib only.
"""

import argparse
import json
import os
import subprocess
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from string import Template

# --- shell ----------------------------------------------------------------
# The only place this glue shells out, so the kubectl surface stays in one
# auditable section.


def call(cmd, *, stdin=None):
    """Run cmd without raising; return the CompletedProcess."""
    return subprocess.run(cmd, input=stdin, capture_output=True, text=True)


def run(cmd, *, stdin=None, check=True):
    """Run cmd and return stdout. Raise on non-zero exit when check is set."""
    p = call(cmd, stdin=stdin)
    if check and p.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({p.returncode}): {p.stderr.strip()}")
    return p.stdout


def run_json(cmd, **kw):
    return json.loads(run(cmd, **kw))


def emit(obj):
    """Write one JSON object as a line to stdout (pipeline-friendly)."""
    print(json.dumps(obj))


# --- manifests ------------------------------------------------------------
# Workload bodies live under manifests/, beside this script -- not in code.

_ROOT = os.path.dirname(os.path.abspath(__file__))


def manifest(rel):
    return os.path.join(_ROOT, "manifests", rel)


# --- pods: batch log-generator pods (replaces k8s-pod-log-generator) ------


def _template():
    with open(manifest("load/log-generator-pod.yaml")) as f:
        return Template(f.read())


def _render(tmpl, namespace, pod_name, total_lines, bytes_per_line):
    # safe_substitute: fills our ${KEYS}, leaves shell $(seq ...) untouched.
    return tmpl.safe_substitute(
        NAMESPACE=namespace,
        POD_NAME=pod_name,
        TOTAL_LOG_LINES=total_lines,
        BYTES_PER_LINE=bytes_per_line,
    )


def _plan(cfg):
    """Return (namespace, pod_name, total_lines) tuples for every pod to create."""
    total_lines = (cfg["kilobytes_per_pod"] * 1024) // cfg["bytes_per_line"]
    num_pods = (cfg["megabytes_total"] * 1024) // cfg["kilobytes_per_pod"]
    nns = cfg["namespace_count"]
    prefix = cfg["namespace_prefix"]
    plan = []
    for i in range(num_pods):
        ns = f"{prefix}-{i % nns}"
        plan.append((ns, f"logger-{i}", total_lines))
    return plan


def _ensure_namespace(name):
    if call(["kubectl", "get", "namespace", name]).returncode != 0:
        run(["kubectl", "create", "namespace", name])


def cmd_pod_up(args, cfg):
    tmpl = _template()
    plan = _plan(cfg)
    bpl = cfg["bytes_per_line"]

    if args.dry_run:
        rendered = []
        for ns, pod, n in plan:
            rendered.append(_render(tmpl, ns, pod, n, bpl))
        print("\n---\n".join(rendered))
        return 0

    namespaces = set()
    for ns, _, _ in plan:
        namespaces.add(ns)
    for ns in sorted(namespaces):
        _ensure_namespace(ns)

    def apply(item):
        ns, pod, n = item
        run(["kubectl", "apply", "-f", "-"], stdin=_render(tmpl, ns, pod, n, bpl))
        return {"namespace": ns, "pod": pod, "total_log_lines": n}

    with ThreadPoolExecutor(max_workers=cfg["concurrency"]) as pool:
        for rec in pool.map(apply, plan):
            emit(rec)
    return 0


def cmd_pod_down(args, cfg):
    prefix = cfg["namespace_prefix"]
    for i in range(cfg["namespace_count"]):
        run(["kubectl", "delete", "namespace", f"{prefix}-{i}", "--ignore-not-found"])
    return 0


# --- task: single Tekton TaskRun (replaces tekton-task-run-creator) -------


def _wait_for_pod(namespace, taskrun_name, *, timeout, poll):
    deadline = time.monotonic() + timeout
    selector = f"tekton.dev/taskRun={taskrun_name}"
    while time.monotonic() < deadline:
        data = run_json(
            ["kubectl", "get", "pods", "-n", namespace, "-l", selector, "-o", "json"]
        )
        items = data.get("items", [])
        if items:
            pod = items[0]
            start = pod.get("status", {}).get("startTime") or pod["metadata"].get(
                "creationTimestamp"
            )
            if start:
                return pod["metadata"]["name"], start
        time.sleep(poll)
    return None, None


def cmd_task_run(args, cfg):
    ns = args.namespace

    run(
        [
            "kubectl",
            "apply",
            "-n",
            ns,
            "-f",
            manifest("tekton/task-random-strings.yaml"),
        ]
    )

    with open(manifest("tekton/taskrun-random-strings.yaml")) as f:
        taskrun_yaml = f.read()
    created = run_json(
        ["kubectl", "create", "-n", ns, "-f", "-", "-o", "json"], stdin=taskrun_yaml
    )
    taskrun_name = created["metadata"]["name"]

    pod_name, start = _wait_for_pod(
        ns, taskrun_name, timeout=args.timeout, poll=args.poll_interval
    )
    if pod_name is None:
        raise RuntimeError(f"pod for taskrun {taskrun_name} did not appear")

    emit(
        {
            "namespace": ns,
            "pod": pod_name,
            "podStartTime": start,
            "taskRunName": taskrun_name,
        }
    )
    return 0


# --- config ---------------------------------------------------------------
# Load shaping only. kubeconfig/context is owned by kubectl, not here.

_CONFIG_DEFAULTS = {
    "namespace_prefix": "logger-ns",
    "namespace_count": 10,
    "bytes_per_line": 40,
    "kilobytes_per_pod": 200,
    "megabytes_total": 5,
    "concurrency": 5,
}


def load_config(path):
    cfg = dict(_CONFIG_DEFAULTS)
    try:
        with open(path, "rb") as f:
            cfg.update(tomllib.load(f))
    except FileNotFoundError:
        pass
    return cfg


# --- cli ------------------------------------------------------------------


def build_parser():
    p = argparse.ArgumentParser(prog="k8s-logload.py")
    p.add_argument("-c", "--config", default="config.toml")
    sub = p.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("pod-up", help="create the log-generator pods")
    up.add_argument(
        "--dry-run",
        action="store_true",
        help="print rendered manifests instead of applying",
    )
    up.set_defaults(func=cmd_pod_up)

    down = sub.add_parser("pod-down", help="delete the log-generator pods")
    down.set_defaults(func=cmd_pod_down)

    tk = sub.add_parser(
        "task-run", help="run the random-strings Tekton Task, emit pod record"
    )
    tk.add_argument("-n", "--namespace", default="default")
    tk.add_argument("--timeout", type=float, default=60.0)
    tk.add_argument("--poll-interval", type=float, default=1.0)
    tk.set_defaults(func=cmd_task_run)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    return args.func(args, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
