#!/usr/bin/env python3
"""Static validation for the simplified AtlasFlow YouTube project."""
from __future__ import annotations
import ast, json, re, sys, subprocess
from pathlib import Path
import yaml
ROOT = Path(__file__).resolve().parents[1]
FAILURES: list[str] = []
CHECKS = 0
def check(condition: bool, message: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"{'PASS' if condition else 'FAIL'} | {message}")
    if not condition: FAILURES.append(message)
def read(rel: str) -> str:
    p=ROOT/rel
    if not p.is_file(): check(False, f"required file exists: {rel}"); return ""
    return p.read_text(encoding='utf-8-sig')
def main() -> int:
    print('AtlasFlow YouTube Edition — static validation')
    compose_text=read('compose.yaml')
    try:
        compose=yaml.safe_load(compose_text); services=compose.get('services',{})
        check(isinstance(services,dict) and len(services)>=15,'Compose file parses and contains the platform services')
    except Exception as exc:
        compose={}; services={}; check(False,f'compose.yaml parses ({exc})')
    expected={'postgres','kafka','connect','object-store','create-warehouse-bucket','iceberg-rest','flink-jobmanager','flink-taskmanager','trino','superset-db','superset-init','superset','prometheus','blackbox-exporter','kafka-exporter','cadvisor','grafana','kafka-ui','kafka-init','pipeline-bootstrap','data-generator','node-exporter','postgres-exporter','loki','alloy'}
    check(expected.issubset(services),'all platform services are present')
    check(not any(str(x).lower().startswith('airf') for x in services),'removed orchestration services are absent from Compose')
    check(compose.get('name')=='atlasflow-youtube','Compose project identity is preserved')
    check(all(x in compose_text for x in ['atlasflow-lite-postgres-data','atlasflow-lite-flink-checkpoints','atlasflow-lite-flink-jobmanager']),'named containers and volumes retain the existing project identity')
    check(all(x in compose_text for x in ['./runtime/bronze:/opt/atlasflow/runtime/bronze:ro','./runtime/silver:/opt/atlasflow/runtime/silver','./runtime/gold:/opt/atlasflow/runtime/gold']) and not any('airf' in line.lower() for line in compose_text.splitlines()),'Compose uses semantic runtime mounts and no removed orchestration volume')
    schema=read('docker/postgres/init/10-atlasflow-schema.sql')
    expected_tables={'customers','orders','inventory_balances','carriers','shipments','tracking_events','disruptions'}
    source_tables=set(re.findall(r'(?im)^CREATE\s+TABLE\s+public\.([a-z_]+)',schema))
    check(source_tables==expected_tables and len(source_tables)==7,'operational PostgreSQL schema still defines exactly seven source tables')
    try:
        cfg=json.loads(read('connectors/postgres-cdc-connector.json'))['config']
        included={i.strip().split('.')[-1] for i in cfg['table.include.list'].split(',')}
        check(included==expected_tables and len(included)==7,'one Debezium connector captures the seven source tables')
        check(cfg.get('transforms.route.replacement')=='atlasflow.cdc.v1','Debezium routes business changes to one CDC topic')
    except Exception as exc: check(False,f'Debezium connector config parses ({exc})')
    generator=read('generators/generate_logistics_data.py')
    try: ast.parse(generator); check(True,'business-event generator Python syntax parses')
    except SyntaxError as exc: check(False,f'business-event generator syntax ({exc})')
    check('INSERT INTO public.tracking_events' in generator and 'INSERT INTO public.disruptions' in generator and 'UPDATE public.shipments' in generator,'generator writes tracking/disruption rows and updates a shipment in PostgreSQL')
    check('KafkaProducer' not in generator and 'confluent_kafka' not in generator,'generator does not bypass PostgreSQL and Debezium')
    files={
      'Bronze':'sql/bronze/01_cdc_to_bronze.sql.template',
      'Silver CDC':'sql/silver/01_cdc_to_silver.sql.template',
      'Gold':'sql/gold/stream_shipment_control_tower.sql.template'}
    for name,rel in files.items():
        body=read(rel); check(bool(body.strip()),f'{name} transformation exists')
        check('CREATE CATALOG atlasflow' in body and '__RUSTFS_ACCESS_KEY__' in body,f'{name} configures the shared Iceberg REST catalog')
        check(len(body.splitlines()) <= 150, f'{name} transformation stays within the 150-line teaching limit')
    check('atlasflow.cdc.v1' in read(files['Bronze']),'Bronze reads only the consolidated CDC topic')
    check('TABLE_SCAN_THEN_INCREMENTAL' in read(files['Silver CDC']),'Silver CDC reads Bronze snapshot then follows appends')
    silver_ddl=read('sql/silver/bootstrap_silver_tables.sql.template')
    check('CREATE TABLE IF NOT EXISTS atlasflow.silver.cdc_changes' in silver_ddl,'Silver CDC history bootstrap remains separate')
    gold_sql=read(files['Gold']); gold_ddl=read('sql/gold/bootstrap_gold_tables.sql.template')
    check("SET 'execution.runtime-mode' = 'streaming';" in gold_sql and 'INSERT INTO atlasflow.gold.shipment_control_tower' in gold_sql and 'INSERT OVERWRITE' not in gold_sql,'Gold uses a continuous streaming INSERT, not a batch overwrite')
    check("'streaming' = 'true'" in gold_sql and 'TABLE_SCAN_THEN_INCREMENTAL' in gold_sql,'Gold scans existing Silver CDC history then follows new appended change records')
    check('PRIMARY KEY (shipment_id) NOT ENFORCED' in gold_ddl and "'write.upsert.enabled' = 'true'" in gold_ddl,'Gold sink supports streaming keyed upserts')
    check(all(x in gold_sql for x in ['customers_model','orders_model','shipments_model','carriers_model','inventory_model','latest_tracking_model','disruption_summary_model']),'Gold derives joins and risk signals continuously from Silver CDC history')
    check('CREATE TEMPORARY VIEW latest_versions' in gold_sql and 'CREATE TEMPORARY VIEW current_rows' in gold_sql,'CDC current-row selection remains temporary rather than an extra persistent table')
    check(not any(p.name.startswith('refresh_') for p in (ROOT/'sql/silver').glob('*.template')),'old batch Silver refresh orchestration is removed')
    for p in ROOT.rglob('*'):
        if not p.is_file(): continue
        rel=p.relative_to(ROOT).as_posix()
        if rel == 'scripts/check-atlasflow-static.py': continue
        if any(seg in {'.venv','venv','node_modules','__pycache__','.git','logs'} for seg in p.parts): continue
        txt=''
        if p.suffix.lower() in {'.md','.sh','.py','.sql','.template','.yml','.yaml','.json','.example'}:
            try: txt=p.read_text(encoding='utf-8-sig',errors='ignore')
            except OSError: continue
            if re.search(r'stage[6789]',txt,re.I): check(False,f'legacy numbered layer reference removed: {rel}')
            if 'airf' in txt.lower(): check(False,f'removed orchestration reference found: {rel}')
        if p.suffix.lower() in {'.yaml','.yml'}:
            try: yaml.safe_load(txt); check(True,f'YAML parses: {rel}')
            except Exception as exc: check(False,f'YAML parses: {rel} ({exc})')
        if p.suffix.lower()=='.json':
            try: json.loads(txt); check(True,f'JSON parses: {rel}')
            except Exception as exc: check(False,f'JSON parses: {rel} ({exc})')
        if p.suffix.lower()=='.py':
            try: ast.parse(txt,filename=rel); check(True,f'Python syntax: {rel}')
            except Exception as exc: check(False,f'Python syntax: {rel} ({exc})')
    env=read('.env.example')
    env_names=set(re.findall(r'(?m)^([A-Z0-9_]+)=',env))
    compose_names=set(re.findall(r'\$\{([A-Z0-9_]+)(?::[-?][^}]*)?\}',compose_text))
    check(compose_names.issubset(env_names),'safe .env.example includes every environment variable referenced by Compose')
    check('.env' in read('.gitignore').splitlines(),'Git ignores local secrets (.env)')
    if '--ci' in sys.argv: check(not (ROOT/'.env').exists(),'archive does not ship .env')
    check(not any(p.name.startswith('generate_tracking') for p in (ROOT/'generators').rglob('*.py')),'obsolete direct tracking generator is deleted')
    check(not any(p.name.lower().startswith('airf') for p in ROOT.iterdir()) and not any(p.name.lower().startswith('airf') for p in (ROOT/'infra').iterdir()),'removed orchestration source/config/log folders are deleted')
    check(not any(p.is_dir() and p.name.startswith('stage') and p.name[5:].isdigit() for p in (ROOT/'sql').iterdir()),'SQL directories use semantic layer names')
    check(not any(p.is_dir() and p.name.startswith('stage') and p.name[5:].isdigit() for p in (ROOT/'runtime').iterdir()),'runtime directories use semantic layer names')
    for rel in ['scripts/submit-streaming-stage.sh','scripts/start-gold-stream.sh','scripts/prepare-local-env.sh','scripts/bootstrap-pipeline.sh','scripts/lib/common.sh','scripts/deploy.sh','scripts/install-docker-ubuntu.sh','infra/generator/Dockerfile','infra/observability/loki/loki-config.yml','infra/observability/alloy/config.alloy','infra/observability/grafana/provisioning/datasources/loki.yml','scripts/run-trino-query-pack.py','scripts/wait-for-cdc-snapshot.py','scripts/wait-for-demo-events.py','scripts/smoke-test-atlasflow.py']:
        check((ROOT/rel).is_file(),f'required helper exists: {rel}')
    check(not any(p.name.startswith('refresh_') for p in (ROOT/'scripts').glob('*.sh')),'old batch Gold refresh script is removed')
    workflow=yaml.safe_load(read('.github/workflows/atlasflow-static-validation.yml'))
    check(isinstance(workflow,dict) and 'validate' in workflow.get('jobs',{}),'GitHub Actions runs static validation')
    check(not any(p.suffix=='.ps1' for p in ROOT.rglob('*') if '.venv' not in p.parts),'no PowerShell scripts remain; shell scripts are used instead')
    check(all(x in compose_text for x in ['grafana/loki:','grafana/alloy:','atlasflow-lite-kafka-data']),'Loki, Alloy and the persistent Kafka volume are in Compose')
    ds=read('infra/observability/grafana/provisioning/datasources/loki.yml')
    check('type: loki' in ds and 'http://loki:3100' in ds,'Grafana provisions Loki as a datasource')
    check('loki.write' in read('infra/observability/alloy/config.alloy'),'Alloy pipeline writes to Loki')
    print(f'\nSummary: {CHECKS} checks, {len(FAILURES)} failure(s)')
    if FAILURES:
        for failure in FAILURES: print('ERROR | '+failure)
        return 1
    if '--ci' not in sys.argv: print('Static checks passed. Live Flink SQL semantics still require a Docker integration run.')
    return 0
if __name__=='__main__': raise SystemExit(main())
