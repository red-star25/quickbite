#!/usr/bin/env bash
# Apply Quickbite manifests.
# --validate=false skips server OpenAPI schema checks (some clusters hide OpenAPI).
# If API discovery fails ("couldn't get current server API group list"), fix kubeconfig
# / context / cluster — that is not something this repo can paper over.
# Build app images first: ./deploy/k8s/build-images.sh (quickbite/orders:local, quickbite/inventory:local).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! kubectl config current-context >/dev/null 2>&1; then
	echo "kubectl has no current context (nothing in ~/.kube/config or none selected)." >&2
	echo "Start or connect to a cluster, then: kubectl config get-contexts && kubectl config use-context <name>" >&2
	echo "Examples: Docker Desktop → enable Kubernetes; minikube start; kind create cluster" >&2
	exit 1
fi

if ! discovery=$(kubectl api-versions 2>&1); then
	printf '%s\n' "$discovery" >&2
	echo >&2
	echo "Quickbite: kubectl cannot list API versions (bad kubeconfig, wrong context, or cluster down)." >&2
	echo "Fix the cluster endpoint, then retry. Examples: kubectl config current-context; kubectl cluster-info" >&2
	exit 1
fi

exec kubectl apply -f "${SCRIPT_DIR}/quickbite.yaml" --validate=false "$@"
