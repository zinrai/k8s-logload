# k8s-logload

Generate a Kubernetes log workload for stressing a logging pipeline: either a
batch of log-emitting Pods, or a Tekton TaskRun that logs. It writes workloads
via `kubectl` and emits JSON pod records on stdout, so a measurer such
as [k8s-loki-logbench](https://github.com/zinrai/k8s-loki-logbench) can consume them by pipe.

A single self-contained Python script (`k8s-logload.py`, stdlib only). It is
**log-backend agnostic**: it does not talk to Loki (or any log store). It only
creates workloads in the cluster; observing where their logs end up is a
separate concern.

## Requirements

- Python 3.11+ (`tomllib`)
- `kubectl` on `PATH` (Tekton Task/TaskRun are CRDs, handled natively)
- kubeconfig / context owned by `kubectl` (`KUBECONFIG`, current-context)

## Subcommands

Run `./k8s-logload.py <cmd> --help` for flags. That output is the source of truth.

| command | what it does |
| --- | --- |
| `pod-up` / `pod-down` | create / delete the log-generator Pods across the configured namespaces |
| `task-run` | apply the `random-strings` Tekton Task, start a TaskRun, emit its pod record |

`pod-up --dry-run` prints the rendered manifests instead of applying them. The
workload bodies live in `manifests/`, not in code. `task-run` needs Tekton
Pipelines installed in the cluster.

## Usage

```bash
./k8s-logload.py pod-up
./k8s-logload.py pod-down

# feed a measurer directly:
./k8s-logload.py task-run | k8s-loki-logbench.py latency --mode tail --stdin
```

## Pod record contract

Producers here and consumers elsewhere meet at one JSON shape, one object per
line:

```json
{"namespace": "...", "pod": "...", "podStartTime": "<RFC3339>"}
```

`pod-up` emits one record per created Pod; `task-run` emits one for the
TaskRun's Pod (plus `taskRunName`). The `total_log_lines` annotation written by
`pod-up` is the source of truth a verifier compares against.

## License

This project is licensed under the [MIT License](./LICENSE).
