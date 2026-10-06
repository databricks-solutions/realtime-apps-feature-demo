#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <databricks-profile> [catalog]" >&2
  exit 2
fi

profile=$1
catalog=${2:-main}
scope=realtime-demo
password=$(openssl rand -hex 24)

if ! databricks secrets list-scopes --profile "$profile" -o json \
  | jq -e --arg scope "$scope" '.scopes[]? | select(.name == $scope)' >/dev/null; then
  databricks secrets create-scope "$scope" --profile "$profile"
fi

for key in lakebase_host lakebase_db lakebase_user lakebase_password; do
  printf '%s' placeholder | databricks secrets put-secret "$scope" "$key" --profile "$profile"
done

export BUNDLE_VAR_catalog=$catalog
export BUNDLE_VAR_pg_password=$password

databricks bundle validate --strict --target dev --profile "$profile"
databricks bundle deploy --target dev --profile "$profile"
databricks bundle run rt_demo_setup_job --target dev --profile "$profile"
databricks bundle run rt_demo_app --target dev --profile "$profile"
databricks bundle run rt_demo_job --target dev --no-wait --profile "$profile"

echo "Deployment complete and the realtime job has started."
echo "The generated database password was stored in secret scope '$scope'."
