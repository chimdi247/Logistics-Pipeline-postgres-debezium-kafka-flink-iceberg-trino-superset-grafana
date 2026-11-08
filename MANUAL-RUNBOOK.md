# AtlasFlow — Manual Runbook

This runbook walks through the complete local AtlasFlow pipeline, from PostgreSQL initialization and CDC capture to the continuously updated Gold table and Superset dashboard.

Run all shell commands from the repository root:

```text
AtlasFlow_YouTube_Edition/
```

The project uses three long-running Flink jobs:

1. `atlasflow-bronze-cdc`
2. `atlasflow-silver-cdc-normalize`
3. `atlasflow-gold-control-tower-stream`

There is no separate Silver current-state model job.

---

## Prerequisites

Make sure the following are installed and running:

- Ubuntu 22.04 / 24.04 with Docker Engine and Docker Compose v2
- Python 3.10 or newer, `jq` and `curl`

> **Automated path:** `docker compose up -d --build` performs Steps 2-6 for you (the `kafka-init`, `pipeline-bootstrap` and `data-generator` services). Use this runbook to run or debug the steps one at a time; the scripts are idempotent and skip jobs that are already RUNNING. If you run them manually, stop the generator first (`docker compose stop data-generator`) if you want a controlled demo.

---

## Step 1 — Prepare the Local Environment and Start Infrastructure

Prepare the local environment:

```bash
./scripts/prepare-local-env.sh
```

This script:

- Creates or updates the local `.env` file.
- Preserves existing non-placeholder credentials.
- Creates the Python virtual environment if needed.
- Installs dependencies from `requirements.txt`.

Start the complete Docker Compose platform:

```bash
docker compose up -d --build
```

Check the containers:

```bash
docker compose ps
```

Wait for the required services to become healthy before continuing.

The stack includes PostgreSQL, Kafka, Kafka Connect, RustFS, Iceberg REST Catalog, Flink, Trino, Superset, Prometheus, Grafana, and supporting monitoring services.

**Important:** Do not delete the existing `.env` file or Docker volumes when restarting an already initialized project. The existing credentials and persistent volumes are part of the local environment.

---

## Step 2 — Prepare PostgreSQL CDC Publication

The PostgreSQL database is named:

```text
atlasflow_ops
```

It contains seven operational source tables:

1. `customers`
2. `orders`
3. `inventory_balances`
4. `carriers`
5. `shipments`
6. `tracking_events`
7. `disruptions`

The schema and initial seed data are created automatically when the PostgreSQL data volume is initialized for the first time.

Create or validate the PostgreSQL publication:

```bash
./scripts/setup-cdc-publications.sh
```

Expected result:

```text
PASS: one PostgreSQL publication covers exactly the seven operational tables.
```

The publication is named:

```text
atlasflow_publication
```

Debezium uses this publication to capture changes from the seven operational tables.

---

## Step 3 — Create the Business Kafka Topic

Create the consolidated business CDC topic:

```bash
./scripts/create-connect-topics.sh
```

The topic is:

```text
atlasflow.cdc.v1
```

The topic is configured with three partitions and a local retention period.

Kafka Connect internal topics are separate infrastructure topics. The application CDC events are routed to the single business topic above.

You can inspect the topic in Kafka UI:

http://localhost:8090

---

## Step 4 — Start Bronze and Register Debezium

Start the Bronze Flink streaming job first:

```bash
./scripts/submit-streaming-stage.sh bronze
```

Expected confirmation:

```text
PASS: atlasflow-bronze-cdc is RUNNING.
```

Starting Bronze before Debezium ensures the Kafka consumer is ready before the initial database snapshot is published.

Now register the Debezium connector:

```bash
./scripts/register-debezium-connectors.sh
```

The connector name is:

```text
atlasflow-postgres-cdc
```

It captures PostgreSQL changes and routes them to:

```text
atlasflow.cdc.v1
```

The initial snapshot contains 34 records:

| Source table | Initial records |
|---|---:|
| customers | 6 |
| orders | 8 |
| inventory_balances | 8 |
| carriers | 4 |
| shipments | 8 |
| **Total** | **34** |

Tracking events and disruption records are generated later by the Python generator.

Wait until the Bronze table contains the initial snapshot:

```bash
.venv/bin/python scripts/wait-for-cdc-snapshot.py --table atlasflow.bronze.cdc_events --minimum-rows 34 --timeout 300
```

Expected confirmation:

```text
PASS: initial snapshot reached atlasflow.bronze.cdc_events (34 rows).
```

The Bronze Iceberg table is:

```text
atlasflow.bronze.cdc_events
```

It stores the raw Debezium event payload together with Kafka metadata such as topic, partition, offset, and ingestion timestamp.

---

## Step 5 — Start Silver CDC Normalization

Start the Silver streaming job:

```bash
./scripts/submit-streaming-stage.sh silver
```

Expected confirmation:

```text
PASS: atlasflow-silver-cdc-normalize is RUNNING.
```

Wait for the initial snapshot to reach Silver:

```bash
.venv/bin/python scripts/wait-for-cdc-snapshot.py --table atlasflow.silver.cdc_changes --minimum-rows 34 --timeout 300
```

Expected confirmation:

```text
PASS: initial snapshot reached atlasflow.silver.cdc_changes (34 rows).
```

The Silver table is:

```text
atlasflow.silver.cdc_changes
```

The Silver transformation:

- Reads the Bronze Iceberg table.
- Starts by scanning existing Bronze data.
- Continues following newly appended Bronze records.
- Extracts source table and operation information from Debezium JSON.
- Normalizes the seven source-table record shapes into a shared schema.
- Derives entity identifiers.
- Converts relevant numeric and timestamp fields.
- Adds a data-quality status.
- Retains Kafka metadata and the original raw payload.

Silver is a normalized CDC change-history layer. It does not create eight additional persistent current-state model tables.

---

## Step 6 — Start the Continuous Gold Control Tower Stream

Make sure the Silver CDC job is RUNNING before starting Gold.

Run:

```bash
./scripts/start-gold-stream.sh
```

Expected confirmation:

```text
PASS: atlasflow-gold-control-tower-stream is RUNNING.
```

The script:

1. Checks that the Silver CDC normalization job is running.
2. Creates the keyed Gold Iceberg table.
3. Submits the Gold streaming SQL transformation.
4. Checks the Flink job status.

The Gold table is:

```text
atlasflow.gold.shipment_control_tower
```

Gold reads the existing Silver CDC history and continues processing new appended CDC records.

Inside its streaming query, Gold creates temporary views to identify the latest valid customer, order, shipment, carrier, inventory, tracking, and disruption records.

It joins the current-state information at shipment level and continuously writes keyed updates to Iceberg using `shipment_id`.

**Why does Gold read Silver CDC history directly?**

The Silver table is append-only CDC history. The Gold streaming query uses that history to reconstruct current state as new source records arrive. This avoids maintaining another set of physical Silver current-state tables.

At this point, the three long-running Flink jobs should be:

| Flink job | Expected state |
|---|---|
| `atlasflow-bronze-cdc` | RUNNING |
| `atlasflow-silver-cdc-normalize` | RUNNING |
| `atlasflow-gold-control-tower-stream` | RUNNING |

You can inspect the jobs in the Flink dashboard:

http://localhost:8081

---

## Step 7 — Generate Logistics Events

The Python generator writes all business changes to PostgreSQL first.

Debezium captures those changes and publishes them to Kafka. They then flow through Bronze and Silver, where the Gold streaming job processes them.

### 7.1 Generate Baseline Events

Run:

```bash
.venv/bin/python generators/generate_logistics_data.py --scenario baseline --count 12 --seed 7
```

This generates 12 tracking events across the existing shipments.

The generator also synchronizes the corresponding shipment status with the generated tracking event. For example, a delivered tracking event is not generated for a shipment that still has an active inventory-shortage condition.

The generator checks and aligns the demonstration inventory quantities when necessary, including when an existing PostgreSQL Docker volume is reused.

### 7.2 Generate the Fault Bundle

Run:

```bash
.venv/bin/python generators/generate_logistics_data.py --scenario fault-bundle
```

This scenario creates a deliberate logistics issue for:

```text
SHP-008
```

It performs the following operations:

- Updates the shipment status to `delayed`.
- Inserts a tracking event with type `exception`.
- Records a 180-minute tracking delay.
- Creates an active high-severity port-congestion disruption at Port of Rotterdam.

The generator uses a stable disruption identifier and handles existing demo disruptions so repeatedly running the fault bundle does not accumulate duplicate active demo records.

### 7.3 Wait for Generated Events to Reach Silver

Run:

```bash
.venv/bin/python scripts/wait-for-demo-events.py --timeout 180
```

Expected confirmation:

```text
PASS: generated PostgreSQL changes have reached Silver.
```

The script verifies that the generated tracking records, disruption records, and delayed shipment update for `SHP-008` are visible in Silver.

The Gold job should continue running and process these updates automatically. No manual Gold refresh or batch transformation is required.

### 7.4 Optional — Run Continuous Baseline Generation

Continuous generation is optional. Use it when demonstrating ongoing changes flowing through the pipeline.

Run:

```bash
.venv/bin/python generators/generate_logistics_data.py --scenario baseline --continuous --count 4 --interval-seconds 30 --seed 7
```

This generates four tracking events per batch and waits 30 seconds before starting the next batch.

Stop the generator with:

```text
Ctrl + C
```

Continuous mode is available only for the baseline scenario. The fault-bundle scenario remains a one-shot operation.

For a controlled demonstration, use the one-shot baseline and fault-bundle commands first. Use continuous mode separately when you want to demonstrate recurring updates.

---

## Step 8 — Connect Trino, Build the Superset Dashboard, and Inspect Observability

### 8.1 Verify the Gold Table Through Trino

Trino is available at:

http://localhost:18080

The catalog is:

```text
atlasflow
```

The Gold schema is:

```text
gold
```

The Gold table is:

```text
shipment_control_tower
```

There is no bundled SQL query pack required for this workflow. You can run the following checks from Superset SQL Lab after creating the Trino connection.

**Check shipment counts by risk level:**

```sql
SELECT
    risk_level,
    COUNT(DISTINCT shipment_id) AS shipment_count
FROM atlasflow.gold.shipment_control_tower
GROUP BY risk_level
ORDER BY shipment_count DESC;
```

**Inspect shipments requiring attention:**

```sql
SELECT
    shipment_id,
    order_id,
    carrier_name,
    shipment_status,
    risk_level,
    primary_risk_reason,
    tracking_delay_minutes,
    inventory_shortage_flag,
    active_disruption_count
FROM atlasflow.gold.shipment_control_tower
WHERE risk_level IN ('HIGH', 'MEDIUM')
ORDER BY
    CASE risk_level
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        ELSE 3
    END,
    tracking_delay_minutes DESC;
```

These queries read the current Gold Iceberg table. They do not start or refresh the streaming jobs.

### 8.2 Connect Superset to Trino

Open:

http://localhost:8088

Sign in using:

- Username: `SUPERSET_ADMIN_USERNAME` from `.env`
- Password: `SUPERSET_ADMIN_PASSWORD` from `.env`

Create a database connection:

1. Navigate to **Settings / Data → Database Connections** or use **+ → Data → Connect Database**, depending on your Superset version.
2. Select **Trino**.
3. Use the following SQLAlchemy URI:

```text
trino://atlasflow@trino:8080/atlasflow/gold
```

The hostname `trino` is the Docker Compose service hostname. Do not replace it with `localhost` inside the Superset connection.

4. Test the connection.
5. Save the database connection.

If a working Trino connection already exists, reuse it rather than creating another one.

### 8.3 Register the Gold Dataset

In Superset:

1. Navigate to **Data → Datasets**.
2. Click **+ Dataset**.
3. Choose the Trino database connection.
4. Select schema `gold`.
5. Select table `shipment_control_tower`.
6. Click **Add**.

All dashboard charts should use this dataset:

```text
atlasflow.gold.shipment_control_tower
```

### 8.4 Create Dashboard Metrics

The following metrics can be added to the dataset's Metrics section.

| Metric | SQL expression |
|---|---|
| `total_shipments` | `COUNT(DISTINCT shipment_id)` |
| `high_risk_shipments` | `SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END)` |
| `inventory_shortage_shipments` | `SUM(CASE WHEN inventory_shortage_flag = TRUE THEN 1 ELSE 0 END)` |
| `active_disruption_links` | `SUM(COALESCE(active_disruption_count, 0))` |

`active_disruption_links` represents the sum of disruption counts associated with shipment rows, not necessarily the number of globally unique disruption incidents.

### 8.5 Recommended Dashboard Charts

Create a dashboard named:

```text
AtlasFlow - Shipment Risk Control Tower
```

Recommended visualizations:

| Chart | Visualization | Configuration |
|---|---|---|
| Total Shipments | Big Number | `total_shipments` |
| High-Risk Shipments | Big Number | `high_risk_shipments` |
| Inventory Shortages | Big Number | `inventory_shortage_shipments` |
| Active Disruption Links | Big Number | `active_disruption_links` |
| Risk Distribution | Donut Chart | Group by `risk_level`, metric `total_shipments` |
| High-Risk Shipments by Carrier | Horizontal Bar | Group by `carrier_name`, metric `high_risk_shipments` |
| Inventory Shortages by Region | Bar Chart | Group by `warehouse_region`, metric `inventory_shortage_shipments` |
| Shipments Requiring Attention | Table | Filter `risk_level` to HIGH and MEDIUM |

For the attention table, include:

- `risk_level`
- `shipment_id`
- `order_id`
- `customer_name`
- `carrier_name`
- `shipment_status`
- `destination_country`
- `latest_tracking_event_type`
- `tracking_delay_minutes`
- `active_disruption_count`
- `expected_disruption_delay_minutes`
- `inventory_shortage_flag`
- `primary_risk_reason`

Use conditional formatting to make HIGH risk red, MEDIUM risk amber, and inventory shortages easy to identify.

Useful dashboard filters include:

- `risk_level`
- `carrier_name`
- `warehouse_region`
- `destination_country`
- `shipment_status`

### 8.6 Dashboard Refresh Behavior

Gold is continuously updated by Flink, but Superset queries Iceberg when a chart or dashboard is refreshed.

This is not a direct push-based visualization.

Use the dashboard refresh control to see the latest committed Gold data. Start with a 30–60 second refresh interval if using automatic refresh, then verify the update frequency and cache behavior in your local environment.

---

## Observability

The platform includes several tools for inspecting pipeline and infrastructure health.

| Tool | URL | What to inspect |
|---|---|---|
| Kafka UI | http://localhost:8090 | Business topic, partitions, messages, and offsets |
| Kafka Connect | http://localhost:8083/connectors | Debezium connector registration and status |
| Flink Dashboard | http://localhost:8081 | Three long-running streaming jobs |
| Iceberg REST | http://localhost:18181/v1/config | Catalog health/configuration |
| Trino | http://localhost:18080 | Query Iceberg-backed tables |
| Superset | http://localhost:8088 | Logistics control tower dashboard |
| Prometheus | http://localhost:9090 | Metrics and scrape targets |
| Grafana | http://localhost:3000 | Provisioned platform-observability dashboard |

For Grafana, use `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` from `.env`.

---

## Troubleshooting

### 1. Initial snapshot does not reach 34 rows

Check:

- PostgreSQL is healthy.
- The CDC publication exists and includes all seven source tables.
- Kafka topic `atlasflow.cdc.v1` exists.
- Debezium connector and task are RUNNING.
- Bronze Flink job is RUNNING.
- Trino is healthy, because the snapshot-wait helper queries through Trino.

Inspect:

```bash
docker compose logs --tail=100 postgres kafka connect trino
```

### 2. Silver does not receive Bronze records

Check that these jobs are RUNNING:

```text
atlasflow-bronze-cdc
atlasflow-silver-cdc-normalize
```

Inspect the Silver SQL client log:

```bash
docker compose exec -T flink-jobmanager sh -lc "tail -n 120 /tmp/atlasflow-lite-silver.log"
```

### 3. Gold does not reach RUNNING

Confirm that Silver CDC normalization is already RUNNING.

Inspect the Gold SQL client log:

```bash
docker compose exec -T flink-jobmanager sh -lc "tail -n 120 /tmp/atlasflow-lite-gold-stream.log"
```

Fix the first Flink SQL error in the log before retrying the Gold startup script.

### 4. Generated events do not reach Silver

Check that the Python generator completed successfully and committed changes to PostgreSQL.

Then inspect the Debezium connector, Kafka topic, Bronze job, and Silver job.

Run the event waiter again:

```bash
.venv/bin/python scripts/wait-for-demo-events.py --timeout 180
```

### 5. Superset dashboard does not reflect new data

Check:

- Gold streaming job is RUNNING.
- New CDC records have reached Silver.
- The Trino connection can query `atlasflow.gold.shipment_control_tower`.
- The Superset dataset points to the correct Gold table.
- The dashboard has been refreshed.
- Chart/dashboard cache settings are not serving stale results.

---

## Stopping the Platform

To stop the containers while preserving the named Docker volumes:

```bash
docker compose down
```

Do not use:

```bash
docker compose down -v
```

unless you intentionally want to delete the persistent PostgreSQL, object-storage, Iceberg catalog, and other project data.

---

## Important Notes

- All source data generation happens in PostgreSQL.
- Debezium is responsible for capturing and publishing database changes.
- Kafka carries the consolidated business CDC stream.
- Bronze stores raw CDC envelopes.
- Silver stores normalized append-only CDC history.
- Gold reconstructs the current shipment view and continuously applies keyed updates.
- Superset reads the Gold table through Trino.
- No separate batch refresh is required for Gold.
- This is a local educational project, not a secured production deployment.

