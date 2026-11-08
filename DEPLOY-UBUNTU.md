# Deploying AtlasFlow on an Ubuntu cloud VM

One Compose file (`compose.yaml`) runs the entire platform: PostgreSQL, Kafka (KRaft), Debezium, RustFS,
Iceberg REST, Flink, Trino, Superset, Prometheus, Grafana, Loki, Alloy and the exporters.

## 1. VM sizing
| | Minimum | Recommended |
|---|---|---|
| vCPU | 4 | 8 |
| RAM | 16 GB | 32 GB |
| Disk | 60 GB SSD | 100 GB |

Ubuntu 22.04 or 24.04. Open **inbound 22 only** in the cloud firewall / security group.

## 2. Install and start
```bash
git clone <your repo> atlasflow && cd atlasflow     # or unzip the project
chmod +x scripts/*.sh scripts/lib/*.sh
./scripts/install-docker-ubuntu.sh                  # re-login afterwards for the docker group
./scripts/deploy.sh                                 # generates .env, builds, starts, smoke-tests
```
First start takes roughly 10-20 minutes (image pulls, Flink jar download, Trino warm-up). `docker compose up -d`
blocks until `pipeline-bootstrap` finishes because the generator depends on it. Follow progress with:
```bash
docker compose logs -f pipeline-bootstrap
docker compose ps
```

### What starts automatically
`kafka-init` creates `atlasflow.cdc.v1` -> `pipeline-bootstrap` creates the publication, submits Bronze, registers
Debezium, waits for the 34-row snapshot, submits Silver, then Gold -> `data-generator` injects the SHP-008 fault
bundle once and then emits baseline events continuously.

## 3. Reach the UIs (SSH tunnel)
Ports are bound to `127.0.0.1` (`BIND_ADDRESS` in `.env`). From your laptop:
```bash
ssh -L 3000:localhost:3000 -L 8088:localhost:8088 -L 8081:localhost:8081 -L 8090:localhost:8090 user@VM_IP
```
Grafana http://localhost:3000, Superset :8088, Flink :8081, Kafka UI :8090. Credentials are in `.env`
(mode 600). Exposing ports publicly requires TLS and an authenticating reverse proxy first; do not simply set
`BIND_ADDRESS=0.0.0.0`.

## 4. Logs and metrics
* **Alloy** reads container logs through the Docker socket (and the host journal), merges multi-line stack traces,
  parses JSON / logfmt / per-service formats, normalises `level` (`error|warn|info|debug|unknown`) and ships to **Loki**.
* Labels: `project, service, container, job, level`. `logger` and `pid` are structured metadata.
* Grafana -> Dashboards -> AtlasFlow: *Logs (Loki)*, *Host, PostgreSQL, Flink & Logging*, *Platform Observability*.
  Query example: `{service="connect", level="error"}`.
* Retention: Loki 15 days, Prometheus 15 days, Docker json-file logs 5 x 20 MB per container.

## 5. Survive reboots
```bash
./scripts/install-systemd-service.sh && sudo systemctl start atlasflow
```
Containers restart by themselves (`restart: unless-stopped`) but a Flink JobManager restart discards the streaming
jobs. The unit re-runs `docker compose up -d`, whose idempotent `pipeline-bootstrap` resubmits them.

## 6. Operations
```bash
docker compose ps                       # health
./scripts/verify-atlasflow.sh           # static + smoke tests
python3 scripts/run-trino-query-pack.py # sample SQL on the lakehouse
docker compose stop data-generator      # pause synthetic events
docker compose down                     # stop (keeps data); `down -v` DELETES all data
```

## 7. Known limitations
* Flink jobs start from the earliest Kafka offset when resubmitted, so a recovery after a JobManager restart can
  duplicate Bronze/Silver history rows (Gold is keyed and stays correct). Savepoint-based restarts are a follow-up.
* Alloy mounts the Docker socket (root-equivalent access); keep the VM single-tenant.
* Single-node Kafka/Loki/Flink: no high availability.
* Debezium JMX metrics are not exported; connector health is covered by the Connect REST status and logs in Loki.
