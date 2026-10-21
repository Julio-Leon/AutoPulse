#!/usr/bin/env bash
# AutoPulse | Deploy Lambda + Glue jobs + IAM roles.
# Thin wrapper around infra/aws/deploy.py (boto3; CloudFormation isn't
# available in this AWS account). All options are passed through, e.g.:
#     bash infra/aws/deploy.sh
#     bash infra/aws/deploy.sh --snowflake-iam-user-arn ... --snowflake-external-id ...
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"
exec python3 infra/aws/deploy.py "$@"
