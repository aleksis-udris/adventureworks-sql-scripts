# Deployment & Operations Runbook

## Table of Contents
1. [Production Deployment](#production-deployment)
2. [Monitoring & Alerts](#monitoring-alerts)
3. [Backup & Recovery](#backup-recovery)
4. [Incident Response](#incident-response)
5. [Maintenance Procedures](#maintenance-procedures)
6. [Scaling Strategies](#scaling-strategies)

---

## Production Deployment

### Pre-Deployment Checklist

**Code Quality**:
- [ ] All DAGs tested in development
- [ ] No syntax errors in Python files
- [ ] All SQL scripts validated
- [ ] Error handling tested
- [ ] Documentation updated

**Infrastructure**:
- [ ] PostgreSQL accessible from Airflow
- [ ] ClickHouse accessible and scaled appropriately
- [ ] Network security configured
- [ ] Firewall rules in place
- [ ] SSL/TLS certificates valid

**Configuration**:
- [ ] Environment variables set for production
- [ ] Fernet key generated and secured
- [ ] Database credentials encrypted
- [ ] Email/Slack alerts configured
- [ ] Log retention policy set

**Data**:
- [ ] Source data validated
- [ ] Schema created in ClickHouse
- [ ] Dimensions pre-loaded
- [ ] Sample queries tested
- [ ] Performance benchmarks met

### Deployment Steps

#### Step 1: Prepare Production Environment

```bash
# Create production directory
mkdir -p /opt/adventureworks-dwh
cd /opt/adventureworks-dwh

# Clone repository
git clone https://github.com/your-org/adventureworks-dwh.git .

# Create production .env file
cp .env.example .env.production

# Edit with production values
nano .env.production
```

**Production .env configuration**:
```bash
# Mark as production
ENVIRONMENT=production
LOG_LEVEL=WARNING

# PostgreSQL Source
POSTGRES_HOST=<production_postgres_host>
POSTGRES_PORT=5432
POSTGRES_USER=<service_account>
POSTGRES_PASS=<encrypted_password>
ADVENTUREWORKS_DATABASE=AdventureWorks

# ClickHouse Warehouse
CLICKHOUSE_HOST=<production_clickhouse_host>
CLICKHOUSE_USER=<service_account>
CLICKHOUSE_PASS=<encrypted_password>
CLICKHOUSE_PORT=8443

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=<production_fernet_key>
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Alerting
AIRFLOW__SMTP__SMTP_HOST=<smtp_server>
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=<email_account>
AIRFLOW__SMTP__SMTP_PASSWORD=<email_password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow-prod@company.com
```

#### Step 2: Deploy Airflow

```bash
# Start services
docker compose --env-file .env.production up -d

# Initialize database
docker compose exec airflow-webserver airflow db init

# Create admin user
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@company.com \
  --password <secure_password>

# Install dependencies
docker compose exec airflow-webserver pip install \
  clickhouse-connect \
  psycopg2-binary \
  python-dotenv

docker compose exec airflow-scheduler pip install \
  clickhouse-connect \
  psycopg2-binary \
  python-dotenv
```

#### Step 3: Deploy Schema to ClickHouse

```bash
# Connect to production ClickHouse
clickhouse-client --host <prod_host> --port 9000 --user <user> --password

# Run schema scripts
SOURCE /path/to/sql/dimension_tables.sql
SOURCE /path/to/sql/fact_tables.sql
SOURCE /path/to/sql/error_table.sql
SOURCE /path/to/sql/aggregation_tables.sql
SOURCE /path/to/sql/analytical_views.sql
SOURCE /path/to/sql/views.sql

# Verify
SHOW DATABASES;
SHOW TABLES FROM ADVENTUREWORKS_DWS;
```

#### Step 4: Enable DAGs

```bash
# Unpause DAGs
docker compose exec airflow-webserver airflow dags unpause adventureworks_dimension_sync
docker compose exec airflow-webserver airflow dags unpause adventureworks_fact_population

# Verify DAGs
docker compose exec airflow-webserver airflow dags list
```

#### Step 5: Initial Data Load

```bash
# Trigger dimension load
docker compose exec airflow-webserver airflow dags trigger adventureworks_dimension_sync

# Wait for completion (monitor logs)
docker compose logs -f airflow-scheduler

# Verify dimensions loaded
clickhouse-client --host <prod_host> --query "SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.DimCustomer"

# Trigger fact load
docker compose exec airflow-webserver airflow dags trigger adventureworks_fact_population

# Monitor progress
docker compose exec airflow-webserver airflow dags list-runs -d adventureworks_fact_population
```

### Post-Deployment Verification

```bash
# Check all services running
docker compose ps

# Check Airflow health
curl http://localhost:8080/health

# Verify data quality
clickhouse-client --host <prod_host> --query "
SELECT 
    SourceTable,
    ErrorType,
    COUNT(*) as error_count
FROM ADVENTUREWORKS_DWS.error_records
WHERE ErrorDate >= today()
GROUP BY SourceTable, ErrorType
"

# Test query performance
clickhouse-client --host <prod_host> --query "
SELECT 
    SalesDateKey,
    COUNT(*) as transactions,
    SUM(SalesRevenue) as revenue
FROM ADVENTUREWORKS_DWS.FactSales
WHERE SalesDateKey >= today() - 7
GROUP BY SalesDateKey
ORDER BY SalesDateKey DESC
" --time

# Should complete in under 1 second
```

---

## Monitoring & Alerts

### Key Metrics to Monitor

**Airflow Metrics**:
- DAG success/failure rate
- Task duration
- Scheduler heartbeat
- Queue depth
- Resource utilization (CPU, memory)

**ClickHouse Metrics**:
- Query performance
- Insert rate
- Disk usage
- Memory usage
- Merge activity

**Data Quality Metrics**:
- Error record count
- Missing FK percentage
- Row count anomalies
- Partition sizes

### Monitoring Queries

**DAG Performance**:
```bash
# Check recent DAG runs
docker compose exec airflow-webserver airflow dags list-runs -d adventureworks_fact_population --state failed

# Check task durations
docker compose exec airflow-webserver airflow tasks states-for-dag-run \
  adventureworks_fact_population latest
```

**ClickHouse Performance**:
```sql
-- Query execution times
SELECT 
    query,
    query_duration_ms / 1000 as duration_sec,
    read_rows,
    formatReadableSize(read_bytes) as read_size
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
  AND query_duration_ms > 1000
ORDER BY query_duration_ms DESC
LIMIT 10;

-- Disk usage by table
SELECT 
    database,
    table,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC;

-- Recent errors
SELECT 
    event_time,
    event_time_microseconds,
    message
FROM system.text_log
WHERE level = 'Error'
  AND event_date >= today()
ORDER BY event_time DESC
LIMIT 20;
```

**Data Quality**:
```sql
-- Error summary
SELECT 
    SourceTable,
    ErrorType,
    ErrorSeverity,
    COUNT(*) as error_count,
    MAX(ErrorDate) as last_error
FROM ADVENTUREWORKS_DWS.error_records
WHERE ErrorDate >= today() - 7
GROUP BY SourceTable, ErrorType, ErrorSeverity
ORDER BY error_count DESC;

-- Row count trends
SELECT 
    SalesDateKey,
    COUNT(*) as row_count,
    SUM(SalesRevenue) as total_revenue
FROM ADVENTUREWORKS_DWS.FactSales
WHERE SalesDateKey >= today() - 30
GROUP BY SalesDateKey
ORDER BY SalesDateKey DESC;
```

### Alert Configuration

**Critical Alerts** (Immediate notification):
- DAG failure after all retries
- ClickHouse connection failure
- PostgreSQL connection failure
- Disk usage > 90%
- Error record count > 1000/day

**Warning Alerts** (Email notification):
- DAG runtime > 2x normal duration
- FK miss rate > 5%
- Query performance degradation > 50%
- Partition size > 50GB

**Example Alert Script**:
```bash
#!/bin/bash
# alert_check.sh - Run every 5 minutes via cron

# Check for failed DAG runs
FAILED_RUNS=$(docker compose exec -T airflow-webserver airflow dags list-runs --state failed -d adventureworks_fact_population --output json | jq '. | length')

if [ "$FAILED_RUNS" -gt 0 ]; then
    echo "CRITICAL: $FAILED_RUNS failed DAG runs" | mail -s "Airflow Alert" ops@company.com
fi

# Check error record count
ERROR_COUNT=$(clickhouse-client --host <prod_host> --query "SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.error_records WHERE ErrorDate >= today()" 2>/dev/null)

if [ "$ERROR_COUNT" -gt 1000 ]; then
    echo "WARNING: $ERROR_COUNT errors today" | mail -s "Data Quality Alert" ops@company.com
fi

# Check ClickHouse disk usage
DISK_USAGE=$(clickhouse-client --host <prod_host> --query "SELECT round(sum(bytes_on_disk) / (SELECT total_space FROM system.disks WHERE name = 'default') * 100, 2) FROM system.parts WHERE active" 2>/dev/null)

if (( $(echo "$DISK_USAGE > 90" | bc -l) )); then
    echo "CRITICAL: Disk usage at ${DISK_USAGE}%" | mail -s "ClickHouse Disk Alert" ops@company.com
fi
```

---

## Backup & Recovery

### Backup Strategy

**ClickHouse Data**:
- **Frequency**: Daily full backups
- **Retention**: 30 days
- **Method**: ClickHouse BACKUP command or filesystem snapshots

**Airflow Metadata**:
- **Frequency**: Daily
- **Retention**: 7 days
- **Method**: PostgreSQL pg_dump

**Configuration Files**:
- **Frequency**: On change
- **Retention**: Version controlled in Git
- **Method**: Git commits

### Backup Procedures

#### ClickHouse Backup

```bash
#!/bin/bash
# clickhouse_backup.sh

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups/clickhouse"

# Create backup
clickhouse-client --host <prod_host> --query "
BACKUP DATABASE ADVENTUREWORKS_DWS 
TO Disk('backups', 'dwh_backup_${DATE}.zip')
"

# Verify backup
if [ $? -eq 0 ]; then
    echo "Backup successful: dwh_backup_${DATE}.zip"
else
    echo "Backup failed" | mail -s "Backup Alert" ops@company.com
    exit 1
fi

# Remove backups older than 30 days
find $BACKUP_DIR -name "dwh_backup_*.zip" -mtime +30 -delete
```

#### Airflow Metadata Backup

```bash
#!/bin/bash
# airflow_backup.sh

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups/airflow"

# Backup Airflow metadata database
docker compose exec -T postgres pg_dump -U airflow airflow | gzip > ${BACKUP_DIR}/airflow_metadata_${DATE}.sql.gz

# Verify
if [ $? -eq 0 ]; then
    echo "Airflow backup successful"
else
    echo "Airflow backup failed" | mail -s "Backup Alert" ops@company.com
    exit 1
fi

# Remove old backups
find $BACKUP_DIR -name "airflow_metadata_*.sql.gz" -mtime +7 -delete
```

### Recovery Procedures

#### Restore ClickHouse Database

```bash
# Stop Airflow DAGs
docker compose exec airflow-webserver airflow dags pause adventureworks_dimension_sync
docker compose exec airflow-webserver airflow dags pause adventureworks_fact_population

# Restore from backup
clickhouse-client --host <prod_host> --query "
RESTORE DATABASE ADVENTUREWORKS_DWS 
FROM Disk('backups', 'dwh_backup_20251222.zip')
"

# Verify restoration
clickhouse-client --host <prod_host> --query "SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.FactSales"

# Resume DAGs
docker compose exec airflow-webserver airflow dags unpause adventureworks_dimension_sync
docker compose exec airflow-webserver airflow dags unpause adventureworks_fact_population
```

#### Restore Airflow Metadata

```bash
# Stop Airflow services
docker compose stop airflow-webserver airflow-scheduler

# Restore database
gunzip < /backups/airflow/airflow_metadata_20251222.sql.gz | \
  docker compose exec -T postgres psql -U airflow airflow

# Restart services
docker compose start airflow-webserver airflow-scheduler

# Verify
docker compose exec airflow-webserver airflow dags list
```

---

## Incident Response

### Incident Severity Levels

**SEV1 - Critical** (Immediate response required):
- Complete system outage
- Data corruption
- Security breach
- Loss of critical data

**SEV2 - High** (Response within 1 hour):
- DAG failures after retries
- Database connection failures
- Significant performance degradation
- High error rates

**SEV3 - Medium** (Response within 4 hours):
- Individual task failures
- Moderate performance issues
- Non-critical errors

**SEV4 - Low** (Response within 24 hours):
- Minor issues
- Optimization opportunities
- Documentation updates

### Incident Response Playbook

#### SEV1: Complete System Outage

**Symptoms**: No DAGs running, Airflow UI inaccessible

**Response**:
```bash
# Check service status
docker compose ps

# Check logs
docker compose logs --tail=100 airflow-webserver
docker compose logs --tail=100 airflow-scheduler

# Restart services
docker compose restart

# If restart fails, rebuild
docker compose down
docker compose up -d

# Verify recovery
curl http://localhost:8080/health
docker compose exec airflow-webserver airflow dags list

# Document incident
echo "Incident: $(date)" >> /var/log/incidents.log
echo "Resolution: Service restart" >> /var/log/incidents.log
```

#### SEV2: DAG Failure

**Symptoms**: DAG shows failed status in UI

**Response**:
```bash
# Check task logs
docker compose exec airflow-webserver airflow tasks log \
  adventureworks_fact_population \
  extract_resolve_and_load_factsales \
  latest

# Check error records
clickhouse-client --host <prod_host> --query "
SELECT * FROM ADVENTUREWORKS_DWS.error_records 
WHERE ErrorDate >= today() 
ORDER BY ErrorDate DESC 
LIMIT 10
"

# Verify connection:
psql -h <postgres_host> -U <user> -d AdventureWorks -c "SELECT 1"

# Clear failed task and retry
docker compose exec airflow-webserver airflow tasks clear \
  adventureworks_fact_population \
  --yes \
  --task-regex extract_resolve_and_load_factsales

# Monitor retry
docker compose logs -f airflow-scheduler
```

#### SEV2: ClickHouse Performance Degradation

**Symptoms**: Queries taking 10x longer than normal

**Response**:
```sql
-- 1. Check running queries
SELECT 
    query_id,
    user,
    query,
    elapsed,
    read_rows,
    memory_usage
FROM system.processes
WHERE elapsed > 10;

-- 2. Kill slow queries if necessary
KILL QUERY WHERE query_id = '<query_id>';

-- 3. Check for merge issues
SELECT 
    database,
    table,
    count() as parts_count,
    sum(rows) as total_rows
FROM system.parts
WHERE active
GROUP BY database, table
HAVING parts_count > 100
ORDER BY parts_count DESC;

-- 4. Force table optimization if too many parts
OPTIMIZE TABLE ADVENTUREWORKS_DWS.FactSales FINAL;

-- 5. Check disk I/O
SELECT 
    metric,
    value
FROM system.asynchronous_metrics
WHERE metric LIKE '%DiskAvailable%';
```

---

## Maintenance Procedures

### Daily Maintenance

```bash
#!/bin/bash
# daily_maintenance.sh - Run at 3 AM

# Check service health
docker compose ps

# Check disk usage
df -h

# Check error logs
docker compose exec airflow-webserver airflow tasks states-for-dag-run \
  adventureworks_fact_population latest

# Rotate logs older than 7 days
find ./airflow/logs -name "*.log" -mtime +7 -delete

# Check ClickHouse health
clickhouse-client --host <prod_host> --query "SELECT version()"
```

### Weekly Maintenance

```bash
#!/bin/bash
# weekly_maintenance.sh - Run Sunday at 2 AM

# Optimize ClickHouse tables
clickhouse-client --host <prod_host> --query "OPTIMIZE TABLE ADVENTUREWORKS_DWS.FactSales FINAL"
clickhouse-client --host <prod_host> --query "OPTIMIZE TABLE ADVENTUREWORKS_DWS.FactPurchases FINAL"
clickhouse-client --host <prod_host> --query "OPTIMIZE TABLE ADVENTUREWORKS_DWS.DimCustomer FINAL"

# Clear old error records (keep 90 days)
clickhouse-client --host <prod_host> --query "
ALTER TABLE ADVENTUREWORKS_DWS.error_records 
DELETE WHERE ErrorDate < today() - 90
"

# Vacuum Airflow metadata
docker compose exec postgres vacuumdb -U airflow -d airflow --analyze

# Review and archive logs
tar -czf /backups/logs/airflow_logs_$(date +%Y%m%d).tar.gz ./airflow/logs
find ./airflow/logs -name "*.log" -mtime +30 -delete
```

### Monthly Maintenance

```bash
#!/bin/bash
# monthly_maintenance.sh - Run 1st of month at 2 AM

# Drop old partitions (keep 24 months)
# Note: For daily partitions, delete old data instead
clickhouse-client --host <prod_host> --query "
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
DELETE WHERE SalesDateKey < today() - INTERVAL 24 MONTH
"

# Update statistics
clickhouse-client --host <prod_host> --query "SYSTEM RELOAD DICTIONARIES"

# Review capacity planning
clickhouse-client --host <prod_host> --query "
SELECT 
    database,
    formatReadableSize(sum(bytes_on_disk)) as size,
    sum(rows) as total_rows
FROM system.parts
WHERE active
GROUP BY database
"

# Generate monthly report
# (Custom script to email metrics summary)
```

---

## Scaling Strategies

### Vertical Scaling (Scale Up)

**When to scale up**:
- CPU usage consistently > 80%
- Memory usage consistently > 80%
- Query response time degrading

**Actions**:
```bash
# Increase Docker resource limits
# Edit docker-compose.yml:
services:
  airflow-webserver:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G

# Restart services
docker compose down
docker compose up -d
```

**Create a distributed table**:
```sql
CREATE TABLE ADVENTUREWORKS_DWS.FactSales_distributed AS ADVENTUREWORKS_DWS.FactSales
ENGINE = Distributed(adventureworks_cluster, ADVENTUREWORKS_DWS, FactSales, rand());
```

### Load Balancing

**Airflow multi-worker setup**:
```yaml
# docker-compose.yml
services:
  airflow-worker-1:
    image: apache/airflow:2.8.0
    command: celery worker -q worker1
    
  airflow-worker-2:
    image: apache/airflow:2.8.0
    command: celery worker -q worker2
```

---

## Summary

**Key Operational Procedures**:
- Daily health checks
- Weekly optimizations
- Monthly partition management
- Automated backups
- Incident response playbooks
- Scaling strategies

**Critical Contacts**:
- On-call rotation: ops@company.com
- Database team: dba@company.com
- DevOps team: devops@company.com

**Next Steps**:
- Review [Performance & Capacity Planning](performance_capacity.md) for optimization
- See [FAQ & Troubleshooting](faq_troubleshooting.md) for common issues
- Check [Error Handling & Monitoring](error_handling.md) for error resolution