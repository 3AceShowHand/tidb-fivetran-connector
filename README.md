# tidb-fivetran-connector

TiDB Cloud source connector and deployer runtime for Fivetran Connector SDK.

## Connector development

```bash
cd tidb-fivetran-connector
python3 -m venv .venv
. .venv/bin/activate
pip install -e . pytest
pytest
```

## Deployer image

The repository also contains a one-shot Kubernetes Job runtime that:

1. reads `DESTINATION_NAME`, `CONNECTION_NAME`, `FIVETRAN_API_KEY_BASE64`, and `CONNECTOR_CONFIG_JSON`
2. runs `fivetran deploy --force`
3. resolves `connection_id` through the Fivetran REST API with `groups(name) -> connections(group_id, schema)`
4. writes the result JSON to stdout and `/dev/termination-log`

### Build

From the monorepo root:

```bash
docker build -f tidb-fivetran-connector/Dockerfile.deployer -t tidb-fivetran-deployer:local .
```

Or use the helper script:

```bash
AWS_PROFILE=full-manager-service-role \
AWS_REGION=us-east-1 \
AWS_ACCOUNT_ID=385595570414 \
ECR_REPOSITORY=tidb-fivetran-deployer \
IMAGE_TAG=0.1.0 \
tidb-fivetran-connector/scripts/build_deployer_image.sh
```

### Run locally

```bash
docker run --rm \
  -e FIVETRAN_API_KEY_BASE64=... \
  -e DESTINATION_NAME=tidb_snowflake \
  -e CONNECTION_NAME=tidb_job_example \
  -e CONNECTOR_VERSION=0.1.0 \
  -e CONNECTOR_CONFIG_JSON='{"storage_uri":"s3://bucket/prefix"}' \
  tidb-fivetran-deployer:local
```

### Kubernetes

See [k8s/deployer-job.example.yaml](./k8s/deployer-job.example.yaml).
