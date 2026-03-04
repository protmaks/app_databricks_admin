# Databricks Cost and Optimization Dashboard

A Streamlit-based dashboard for monitoring and optimizing Databricks workspace usage, costs, and cluster/job activity. Deployed as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html).

## Features

### Clusters

- **All-Purpose Clusters** — View all non-job clusters with status, worker counts, estimated DBU/hr, auto-termination settings, and uptime. Edit auto-termination timeouts directly from the UI.
- **SQL Warehouses** — Same operational view for SQL Warehouses, including inline auto-stop configuration.
- **All-Purpose Daily Runs** — Gantt chart of cluster state transitions (STARTING, RUNNING, RESTARTING, etc.) for a selected date, plus a bar chart of total daily runtime over the last 90 days.

### Jobs

- **Jobs Timeline** — Gantt chart of all job runs for a selected date with a concurrency chart (5-minute buckets). Overlays projected scheduled runs from cron expressions for jobs that haven't executed yet.
- **Jobs Daily Runs** — Scatter plot of job run history (SUCCESS/FAILED) over a configurable lookback period (1-90 days).
- **Cluster Jobs** — Combined view for a specific cluster: its state timeline, job runs on that cluster, and concurrency — all on a shared time axis.

All pages support timezone selection across 10 common zones.

## Deployment to Databricks Apps

### Prerequisites

- A Databricks workspace with Apps enabled
- The app's service principal must be added to the **Admin** group in your workspace (required for listing clusters, warehouses, and job runs)

### Deploy

1. Install the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)
2. Deploy the app:
   ```bash
   databricks apps deploy <app-name> --source-code-path .
   ```
3. The `app.yaml` configures the Streamlit command and environment variables automatically. The `DATABRICKS_WAREHOUSE_ID` is resolved from the app environment.

### Authentication

When deployed as a Databricks App, authentication is handled automatically via the platform's M2M OAuth — no explicit credentials are needed.

## Local Development

1. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your credentials:
   ```
   DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
   DATABRICKS_TOKEN=<your-personal-access-token>
   DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>
   ```

3. Run the app:
   ```bash
   streamlit run app.py
   ```

### Tests

```bash
pytest tests/
```
