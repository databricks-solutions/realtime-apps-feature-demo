#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: $0 <databricks-profile> [catalog] [target: dev|prod]" >&2
  exit 2
fi

profile=$1
catalog=${2:-main}
target=${3:-dev}
scope=realtime-demo

# The init job writes the `lakebase_endpoint` secret here. The app binds no
# secrets (it uses OAuth), so there are no keys to pre-seed. `list-scopes`
# output is a bare array or `{"scopes": [...]}` depending on CLI version.
if ! databricks secrets list-scopes --profile "$profile" -o json \
  | jq -e --arg scope "$scope" '(.scopes? // .)[]? | select(.name == $scope)' >/dev/null; then
  databricks secrets create-scope "$scope" --profile "$profile"
fi

export BUNDLE_VAR_catalog=$catalog

databricks bundle validate --strict --target "$target" --profile "$profile"
# Deploys the Lakebase project, checkpoint volume, both jobs, and the app. The
# continuous stream job starts right away and retries until init has run.
databricks bundle deploy --target "$target" --profile "$profile"
# One-time, idempotent setup: events table, autovacuum, app SP grant, secret.
databricks bundle run rt_demo_init_job --target "$target" --profile "$profile"
databricks bundle run rt_demo_app --target "$target" --profile "$profile"

echo "Deployment complete. The continuous realtime stream starts automatically;"
echo "the app shows live latency once its first rows land."
