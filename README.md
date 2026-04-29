# bike_sharing_bundle

Databricks Asset Bundle (DABs) project for the bike sharing data platform.

## Layout

- `databricks.yml` — bundle entrypoint and target definitions.
- `resources/` — one YAML per Databricks resource (jobs, pipelines).
- `src/` — source code grouped by the asset that owns it.
- `tests/` — pytest unit tests.
- `.github/workflows/` — CI/CD pipelines for GitHub Actions.

## Resources

| Resource | Type | Original Name |
|---|---|---|
| `bike_pipeline` | Pipeline (SDP) | `dbdemos_pipeline_bike_icalvo_hackaton_ws_catalog_ivandb2` |
| `bike_init_job` | Job | `dbdemos_job_bike_init_ivan_calvo` |

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
databricks bundle validate
```

## Deploying

Production deploys run through CI only — never `databricks bundle deploy -t prod` from a laptop.

| Target | Trigger |
|---|---|
| `dev` | `databricks bundle deploy -t dev` from your machine |
| `staging` | merge to `main` (CI) |
| `prod` | tag/release with manual approval (CI) |

## Variables to configure

Before the first deploy, set these in `databricks.yml` or via CLI overrides:

- `${var.catalog}` — Unity Catalog name (default: `icalvo_hackaton_ws_catalog`)
- `${var.staging_sp_app_id}` — Service principal for staging `run_as`
- `${var.prod_sp_app_id}` — Service principal for production `run_as`
- `${var.warehouse_id}` — if any SQL tasks reference a warehouse
- `${var.notification_email}` — email for job failure alerts

## CI/CD secrets (GitHub)

Configure these in your repository's Settings > Secrets and variables > Actions:

- `DATABRICKS_HOST_STAGING` — workspace URL for staging
- `DATABRICKS_HOST_PROD` — workspace URL for production
- `DATABRICKS_TOKEN` — personal access token or service principal token (prefer OIDC federation)
