# Setup & Installation Guide

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [PostgreSQL Configuration](#postgresql-configuration)
4. [ClickHouse Connection](#clickhouse-connection)
5. [Airflow Setup on WSL](#airflow-setup-on-wsl)
6. [Initial Data Load](#initial-data-load)
7. [Verification](#verification)
8. [Troubleshooting Setup](#troubleshooting-setup)

---

## Prerequisites

### System Requirements

**Minimum**:
- CPU: 4 cores
- RAM: 16 GB
- Disk: 50 GB free space
- OS: Windows 10/11 with WSL2 (Ubuntu 20.04+)

**Recommended**:
- CPU: 8 cores
- RAM: 32 GB
- Disk: 100 GB SSD
- OS: Windows 11 with WSL2 (Ubuntu 22.04)

### Software Requirements

**On Windows**:
- PostgreSQL 15+ (local installation)
- Windows Terminal or PowerShell

**In WSL**:
- Docker 24.0+
- Docker Compose 2.20+
- Python 3.11+
- Git 2.30+

### Check Existing Installation

**On Windows**:
```powershell
# Check PostgreSQL
psql --version

# Test PostgreSQL connection
psql -U postgres -c "SELECT version();"
```

**In WSL**:
```bash
# Check versions
docker --version
docker compose version
python3 --version
git --version

# Check resources
free -h  # Check RAM
df -h    # Check disk space
```

---

## Environment Setup

### Step 1: Install WSL2 (if not already installed)

**On Windows PowerShell (Administrator)**:
```powershell
# Enable WSL
wsl --install

# Install Ubuntu
wsl --install -d Ubuntu-22.04

# Set WSL2 as default
wsl --set-default-version 2

# Verify
wsl --list --verbose
```

### Step 2: Configure WSL

**In WSL terminal**:
```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    git \
    python3 \
    python3-pip \
    postgresql-client
```

### Step 3: Install Docker in WSL

```bash
# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Restart WSL to apply group changes
exit
# Then reopen WSL terminal

# Verify Docker
docker run hello-world
```

### Step 4: Clone Repository

```bash
# Navigate to home directory
cd ~

# Clone project
git clone https://github.com/your-org/adventureworks-dwh.git
cd adventureworks-dwh

# Verify structure
ls -la
# Should see: dags/, sql/, docker-compose.yml, .env.example, README.md
```

### Step 5: Configure Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit configuration
nano .env  # or use: vim .env, code .env
```

**Required `.env` configuration**:

```bash
# PostgreSQL Source Database (Windows local)
# Use host.docker.internal to access Windows host from WSL Docker
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_USER=<your_postgres_user>
POSTGRES_PASS=<your_postgres_password>
ADVENTUREWORKS_DATABASE=AdventureWorks

# ClickHouse Data Warehouse (Remote)
CLICKHOUSE_HOST=<your_remote_clickhouse_host>
CLICKHOUSE_USER=<your_clickhouse_user>
CLICKHOUSE_PASS=<your_clickhouse_password>
CLICKHOUSE_PORT=8443

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=<generate_fernet_key>
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Airflow Webserver
AIRFLOW_WEBSERVER_PORT=8080
AIRFLOW_WEBSERVER_USER=<your_admin_username>
AIRFLOW_WEBSERVER_PASSWORD=<your_admin_password>
AIRFLOW_WEBSERVER_EMAIL=<your_email>

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO
```

**Generate Fernet Key**:
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Copy output to AIRFLOW__CORE__FERNET_KEY in .env
```

**Note**: Replace all values in angle brackets with your actual configuration values. Store sensitive credentials securely.

### Step 6: Create Required Directories

```bash
# Create Airflow directories
mkdir -p ./airflow/dags
mkdir -p ./airflow/logs
mkdir -p ./airflow/plugins
mkdir -p /tmp/airflow

# Set permissions
chmod -R 755 ./airflow
chmod -R 777 /tmp/airflow

# Copy DAG files
cp dags/*.py ./airflow/dags/
```

---

## PostgreSQL Configuration

### Step 1: Install PostgreSQL on Windows

**Download and Install**:
1. Download PostgreSQL 15+ from https://www.postgresql.org/download/windows/
2. Run installer
3. Set password for postgres user
4. Keep default port (5432)
5. Complete installation

### Step 2: Create AdventureWorks Database

**On Windows, open Command Prompt or PowerShell**:

```powershell
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE "AdventureWorks";

# Exit
\q
```

### Step 3: Load Sample Data

**Download AdventureWorks data**:
```powershell
# Download sample data (adjust URL as needed)
Invoke-WebRequest -Uri "https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip" -OutFile "AdventureWorks.zip"

# Extract and load data
# Follow specific instructions for your PostgreSQL version
```

**Or load from SQL script**:
```powershell
psql -U postgres -d AdventureWorks -f path\to\adventureworks_postgresql.sql
```

### Step 4: Configure PostgreSQL for Network Access

**Edit pg_hba.conf** (usually in C:\Program Files\PostgreSQL\15\data\):

```
# Add this line to allow connections from Docker containers
host    all             all             172.16.0.0/12           md5
```

**Edit postgresql.conf**:
```
# Enable listening on all interfaces
listen_addresses = '*'
```

**Restart PostgreSQL service**:
```powershell
# In PowerShell (Administrator)
Restart-Service postgresql-x64-15
```

### Step 5: Test Connection from WSL

```bash
# Test connection to Windows PostgreSQL from WSL
psql -h host.docker.internal -U postgres -d AdventureWorks -c "SELECT COUNT(*) FROM Sales.SalesOrderHeader;"

# Should return row count successfully
```

---

## ClickHouse Connection

### Step 1: Verify Remote ClickHouse Access

**Test connection**:
```bash
# Install ClickHouse client in WSL (optional)
sudo apt install -y clickhouse-client

# Test connection
clickhouse-client \
  --host <your_remote_host> \
  --port 9000 \
  --user <your_user> \
  --password

# Or test with query
clickhouse-client \
  --host <your_remote_host> \
  --port 9000 \
  --user <your_user> \
  --password \
  --query "SELECT version()"
```

### Step 2: Create ClickHouse Schema

**Connect to ClickHouse**:
```bash
clickhouse-client \
  --host <your_remote_host> \
  --port 9000 \
  --user <your_user> \
  --password
```

**Run schema creation scripts**:
```sql
-- Create databases
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DWS;
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DS;
```

**Or run from SQL files**:
```bash
# Navigate to project directory
cd ~/adventureworks-dwh

# Run schema scripts
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/dimension_tables.sql
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/fact_tables.sql
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/error_table.sql
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/aggregation_tables.sql
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/analytical_views.sql
clickhouse-client --host <host> --port 9000 --user <user> --password < sql/views.sql
```

**Verify schema**:
```sql
-- Check databases
SHOW DATABASES;

-- Check tables in warehouse
SHOW TABLES FROM ADVENTUREWORKS_DWS;

-- Check table structure
DESCRIBE TABLE ADVENTUREWORKS_DWS.DimCustomer;
DESCRIBE TABLE ADVENTUREWORKS_DWS.FactSales;
```

---

## Airflow Setup on WSL

### Step 1: Start Airflow Services

**Docker Compose file** (docker-compose.yml):

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  airflow-webserver:
    image: apache/airflow:2.8.0
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - /tmp/airflow:/tmp/airflow
    ports:
      - "8080:8080"
    command: webserver
    extra_hosts:
      - "host.docker.internal:host-gateway"

  airflow-scheduler:
    image: apache/airflow:2.8.0
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - /tmp/airflow:/tmp/airflow
    command: scheduler
    extra_hosts:
      - "host.docker.internal:host-gateway"

volumes:
  postgres-db-volume:
```

**Start services**:
```bash
# Start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

### Step 2: Initialize Airflow

```bash
# Initialize Airflow database
docker compose exec airflow-webserver airflow db init

# Create admin user
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password <your_password>
```

### Step 3: Install Python Dependencies

```bash
# Install required packages in Airflow containers
docker compose exec airflow-webserver pip install \
  clickhouse-connect \
  psycopg2-binary \
  python-dotenv

docker compose exec airflow-scheduler pip install \
  clickhouse-connect \
  psycopg2-binary \
  python-dotenv

# Restart services to apply changes
docker compose restart
```

### Step 4: Verify Airflow Access

**On Windows, open browser**:
- Navigate to: http://localhost:8080
- Login with admin credentials
- Verify DAGs appear in UI

---

## Initial Data Load

### Step 1: Verify DAG Availability

```bash
# List DAGs
docker compose exec airflow-webserver airflow dags list

# Should show:
# - adventureworks_dimension_sync
# - adventureworks_fact_population

# Test DAG files for errors
docker compose exec airflow-webserver python /opt/airflow/dags/adventureworks_dimension_tables.py
docker compose exec airflow-webserver python /opt/airflow/dags/adventureworks_fact_tables.py
```

### Step 2: Enable DAGs

**Via CLI**:
```bash
docker compose exec airflow-webserver airflow dags unpause adventureworks_dimension_sync
docker compose exec airflow-webserver airflow dags unpause adventureworks_fact_population
```

**Or via UI**:
1. Go to http://localhost:8080
2. Toggle ON for both DAGs

### Step 3: Initial Dimension Load

```bash
# Trigger dimension sync manually
docker compose exec airflow-webserver airflow dags trigger adventureworks_dimension_sync

# Monitor progress
docker compose exec airflow-webserver airflow dags list-runs -d adventureworks_dimension_sync

# Watch logs
docker compose logs -f airflow-scheduler
```

**Expected Duration**: 10-15 minutes for initial dimension load.

**Verify dimension data**:
```bash
# Connect to ClickHouse from WSL
clickhouse-client --host <your_remote_host> --port 9000 --user <user> --password

# Check counts
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.DimCustomer;
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.DimProduct;
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.DimStore;
```

### Step 4: Initial Fact Load

```bash
# Trigger fact population (AFTER dimensions complete)
docker compose exec airflow-webserver airflow dags trigger adventureworks_fact_population

# Monitor progress
docker compose exec airflow-webserver airflow dags list-runs -d adventureworks_fact_population
```

**Expected Duration**: 30-60 minutes for initial fact load.

**Verify fact data**:
```sql
-- Check fact counts
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.FactSales;
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.FactPurchases;
SELECT COUNT(*) FROM ADVENTUREWORKS_DWS.FactInventory;
```

### Step 5: Verify Aggregates

```sql
-- Check aggregate tables populated
SELECT COUNT(*) FROM ADVENTUREWORKS_DS.agg_daily_sales;

-- Test aggregate query
SELECT 
    SalesDateKey, 
    SUM(RevenueSum) as total_revenue
FROM ADVENTUREWORKS_DS.agg_daily_sales
GROUP BY SalesDateKey
ORDER BY SalesDateKey DESC
LIMIT 10;
```

---

## Verification

### System Health Checks

```bash
# Check all containers running
docker compose ps

# Check Airflow health
curl http://localhost:8080/health

# Check PostgreSQL connection from WSL
psql -h host.docker.internal -U postgres -d AdventureWorks -c "SELECT 1"

# Check ClickHouse connection
clickhouse-client --host <remote_host> --port 9000 --user <user> --password --query "SELECT 1"
```

### Data Quality Checks

```sql
-- Connect to ClickHouse
clickhouse-client --host <remote_host> --port 9000 --user <user> --password

-- Check for errors
SELECT 
    SourceTable,
    ErrorType,
    COUNT(*) as error_count
FROM ADVENTUREWORKS_DWS.error_records
WHERE ErrorDate >= today() - 1
GROUP BY SourceTable, ErrorType;

-- Should show minimal or zero errors
```

### Query Performance Test

```sql
-- Test query on fact table
SELECT 
    toDate(SalesDateKey) as date,
    COUNT(*) as transactions,
    SUM(SalesRevenue) as revenue
FROM ADVENTUREWORKS_DWS.FactSales
WHERE SalesDateKey >= today() - 30
GROUP BY date
ORDER BY date DESC;

-- Should complete in under 1 second
```

---

## Troubleshooting Setup

### Issue: Cannot connect to PostgreSQL from Docker

**Symptoms**: Connection refused to host.docker.internal

**Solution**:
```bash
# Verify host.docker.internal resolution
docker compose exec airflow-webserver ping host.docker.internal

# If ping fails, check Docker Desktop WSL integration
# Docker Desktop → Settings → Resources → WSL Integration
# Enable integration with your WSL distribution

# Alternative: Use Windows host IP
# In Windows PowerShell:
ipconfig
# Find "Ethernet adapter vEthernet (WSL)" IPv4 address
# Use this IP instead of host.docker.internal in .env
```

### Issue: PostgreSQL not accepting connections

**Symptoms**: "connection refused" or "no pg_hba.conf entry"

**Solution**:
```powershell
# On Windows, edit pg_hba.conf
# Add line:
host    all             all             172.16.0.0/12           md5

# Edit postgresql.conf
listen_addresses = '*'

# Restart PostgreSQL service
Restart-Service postgresql-x64-15
```

### Issue: Airflow containers exit immediately

**Symptoms**: Containers start then stop

**Solution**:
```bash
# Check logs
docker compose logs airflow-webserver

# Common fixes:
# 1. Initialize database first
docker compose exec airflow-webserver airflow db init

# 2. Check Fernet key in .env
# 3. Verify .env file is in same directory as docker-compose.yml

# 4. Fix permissions
chmod -R 777 ./airflow/logs
chmod -R 755 ./airflow/dags
```

### Issue: DAGs not appearing in UI

**Symptoms**: Empty DAG list in Airflow UI

**Solution**:
```bash
# Check DAG directory mounted correctly
docker compose exec airflow-webserver ls -la /opt/airflow/dags

# Check for Python errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Check scheduler logs
docker compose logs airflow-scheduler

# Restart scheduler
docker compose restart airflow-scheduler
```

### Issue: Cannot access ClickHouse

**Symptoms**: Connection timeout or refused

**Solution**:
```bash
# Verify network connectivity
ping <your_remote_host>

# Check port accessibility
nc -zv <your_remote_host> 9000
nc -zv <your_remote_host> 8443

# Verify credentials in .env
cat .env | grep CLICKHOUSE

# Test with clickhouse-client
clickhouse-client --host <host> --port 9000 --user <user> --password
```

### Issue: Slow initial data load

**Symptoms**: Load takes several hours

**Solution**:
```bash
# Increase Docker resources
# Docker Desktop → Settings → Resources
# Increase CPU: 4+ cores
# Increase Memory: 8GB+

# Check WSL memory allocation
# Create/edit .wslconfig in Windows user directory
[wsl2]
memory=16GB
processors=4

# Restart WSL
wsl --shutdown
# Then reopen WSL

# Optimize PostgreSQL
# In postgresql.conf:
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB
```

---

## Post-Installation Steps

### 1. Configure Scheduled Runs

**Schedules are already configured in DAGs**:
- Dimension sync: Hourly
- Fact population: Hourly

**Verify schedules active**:
```bash
docker compose exec airflow-webserver airflow dags list
```

### 2. Set Up Monitoring (Optional)

Configure email alerts in .env if desired, then restart services.

### 3. Access Points

**From Windows**:
- Airflow UI: http://localhost:8080
- ClickHouse: Remote host via client or web interface

**From WSL**:
- PostgreSQL: psql -h host.docker.internal -U postgres -d AdventureWorks
- ClickHouse: clickhouse-client --host <remote_host>

---

## Summary

**Installation Checklist**:
- WSL2 installed and configured
- Docker installed in WSL
- PostgreSQL running on Windows with sample data
- ClickHouse remote connection verified
- Environment variables configured
- Airflow containers running
- DAGs deployed and enabled
- Initial data loaded successfully
- Verification tests passed

**Architecture**:
- PostgreSQL: Windows local (host.docker.internal)
- Airflow: WSL Docker containers
- ClickHouse: Remote server

**Next Steps**:
- See [Deployment Runbook](deployment_runbook.md) for production deployment
- See [FAQ & Troubleshooting](faq_troubleshooting.md) for common issues
- See [Performance & Capacity Planning](performance_capacity.md) for optimization