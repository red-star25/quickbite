#!/usr/bin/env bash
# Build service images expected by quickbite.yaml. Run from repo root:
#   ./deploy/k8s/build-images.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

docker build -f services/orders/Dockerfile -t quickbite/orders:local .
docker build -f services/inventory/Dockerfile -t quickbite/inventory:local .
echo "Built quickbite/orders:local and quickbite/inventory:local"
