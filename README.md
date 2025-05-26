# 📊 Marketing AI/ML Data Engineering Pipeline

This repository contains a scalable, production-style data pipeline built to simulate a Salesforce marketing engagement data ecosystem. The solution includes synthetic data generation, cleansing, Iceberg formatting, Snowflake loading, and transformation using dbt — all orchestrated via Apache Airflow and enabled with CI/CD pipelines.

---

## 📌 Components Overview

| Component     | Technology Used                              | Purpose                                                   |
|---------------|----------------------------------------------|-----------------------------------------------------------|
| Data Gen      | AWS Lambda + Python                          | Generate synthetic Salesforce marketing data              |
| Raw Storage   | Amazon S3                                    | Stores timestamped Parquet files                          |
| Cleaning      | AWS Glue                                     | Cleans nulls, deduplicates, filters malformed records     |
| Processing    | AWS Glue                                     | Converts data to Apache Iceberg format                    |
| Metadata Mgmt | AWS DynamoDB                                 | Tracks last processed timestamps for incremental loads    |
| Warehouse     | Snowflake                                    | Stores RAW → STAGING → DIM/FACT tables                    |
| Transformation| dbt (hosted in EC2)                          | Creates staging, dimensions, facts with lineage           |
| Orchestration | Apache Airflow (EC2)                         | Schedules and monitors the entire pipeline                |
| CI/CD         | GitHub + Docker + CodeBuild + CloudFormation | Automates build and deployment pipelines                  |
| Notifications | Slack + Email                                | Sends job success/failure alerts                          |

---

## 🧩 Installation & Setup

### 1. **Environment Prerequisites**

- Python 3.8+
- Docker
- AWS CLI configured (`aws configure`)
- Apache Airflow 2.x (installed via Docker or EC2)
- dbt-core >= 1.0 (in EC2 or container)
- Snowflake account + credentials

### 2. **Python Dependencies**

Install with:

```bash
pip install -r lambda/requirements.txt
```

Typical libraries include:
- `boto3`
- `pyarrow`
- `pandas`
- `snowflake-connector-python`
- `pytest`, `moto` (for unit testing)

### 3. **Glue Job Setup**

Jobs are written in PySpark. Upload them via AWS Console or boto3.

Each job should be associated with:
- A default IAM role with S3/Glue/Parameter Store access
- S3 location for temporary files
- Parameter for `last_updated_timestamp`

### 4. **DynamoDB Setup**

Each source table (e.g., CAMPAIGN, EMAIL_ENGAGEMENT) has a dedicated table to store `last_updated_timestamp`.

```json
{
  "TableName": "campaign_metadata",
  "Item": {
    "table_name": "CAMPAIGN",
    "last_updated_timestamp": "2024-01-01T00:00:00Z"
  }
}
```

### 5. **Snowflake Configuration**

In `configs/params.json` or `profiles.yml`:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: your_password
      role: your_role
      warehouse: your_warehouse
      database: your_db
      schema: raw
```

### 6. **dbt Usage**

Run inside EC2 or container:

```bash
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## 🚀 Airflow DAG Setup

1. Place DAG in `dags/` folder
2. Use Airflow Variables to define:

```json
{
  "campaign_last_updated": "2024-01-01T00:00:00Z"
}
```

3. Configure connections:
- AWS (aws_default)
- Snowflake
- Slack webhook

4. Trigger manually or schedule via CRON.

---

## 🔐 Secrets & Config

- Use AWS SSM Parameter Store for storing last run timestamps
- Use `.env` file or Airflow connections for API keys and DB credentials
- All sensitive configs should be injected during runtime via Docker/env vars

---

## ✅ Testing

### Unit Testing (Lambda):
```bash
cd lambda/
pytest
```

### Libraries used:
- `pytest` — for assertions and test execution
- `moto` — for mocking S3/DynamoDB
- `unittest.mock` — for mocking time, env, etc.

---

## 📦 CI/CD Pipeline

Each component is deployed via Docker:
- `Dockerfile.lambda`, `Dockerfile.dbt`, `Dockerfile.airflow`
- GitHub webhook triggers AWS CodeBuild on push
- Build and deploy automatically

---

## 📣 Notifications

- Success/failure alerts are sent to Slack using webhook
- Critical failures are emailed via SMTP integration from Airflow

---

## 🧠 Data Modeling

Refer to the following fact tables:

| Fact Table | Purpose |
|------------|---------|
| FACT_MARKETING_PERFORMANCE | Granular marketing engagement |
| FCT_DAILY_PERFORMANCE | Daily campaign performance |
| FCT_CAMPAIGN_SUMMARY | High-level campaign metrics |
| FCT_LEAD_TO_OPPORTUNITY_CONVERSION | Funnel conversion analytics |

All tables are documented and visualized using `dbt docs`.


## 🖥️ EC2 Setup Instructions

### 1. **Launch EC2 Instance**
- Type: t3.medium (recommended)
- AMI: Amazon Linux 2 or Ubuntu 20.04
- Storage: At least 30GB
- Enable ports: 22 (SSH), 8080 (Airflow), 8000 (dbt docs)

### 2. **SSH into EC2**
```bash
ssh -i "your-key.pem" ec2-user@your-public-dns
```

---

## 🐍 Virtual Environment Setup

Create isolated Python environments for Airflow and dbt.

```bash
sudo yum update -y
sudo yum install -y python3 git
python3 -m venv venv
source venv/bin/activate
```

---

## 📦 Pip Installations

### Airflow:
```bash
pip install apache-airflow==2.6.3 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.8.txt"
```

### dbt + Snowflake:
```bash
pip install dbt-core dbt-snowflake
```

### General Dependencies:
```bash
pip install pandas boto3 pyarrow snowflake-connector-python python-dotenv
pip install moto pytest
```

---

## 🧪 Running Locally on EC2

### Activate venv and run Airflow:
```bash
source venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### Run dbt locally:
```bash
cd ~/dbt_project
dbt run
dbt test
dbt docs generate && dbt docs serve --port 8000
```

Ensure ports 8080 and 8000 are allowed in your security group for web UI access.

---

## 🔐 EC2 Networking & Security Configuration

### 1. **Security Group Settings**
When launching your EC2 instance, attach a Security Group with the following **inbound** and **outbound** rules:

#### 🔽 Inbound Rules
| Type          | Protocol | Port Range | Source       | Description                   |
|---------------|----------|------------|--------------|-------------------------------|
| SSH           | TCP      | 22         | Your IP      | For SSH access                |
| HTTP          | TCP      | 8080       | 0.0.0.0/0    | Airflow webserver access      |
| HTTP (Custom) | TCP      | 8000       | 0.0.0.0/0    | dbt docs UI                   |
| All traffic   | TCP      | All        | Your VPC CIDR| Internal service communication|

#### 🔼 Outbound Rules
| Type        | Protocol | Port Range | Destination | Description                  |
|-------------|----------|------------|-------------|------------------------------|
| All traffic | All      | All        | 0.0.0.0/0   | Allow full outbound access   |

---

### 2. **VPC & Subnet Configuration**
Ensure your EC2 is deployed in a VPC with the following settings:

- **Subnet**: Use a public subnet (has route to Internet Gateway)
- **Route Table**: Includes a route:
  ```
  Destination: 0.0.0.0/0
  Target: Internet Gateway (igw-xxxxxxxx)
  ```

### 3. **Internet Gateway**
Attach an Internet Gateway to your VPC for internet access (required for pip installs, Airflow plugins, dbt dependencies, etc).

---

### 4. **Airflow UI and dbt Docs Access**
To access from your browser:

- Airflow: `http://<EC2-Public-IP>:8080`
- dbt Docs: `http://<EC2-Public-IP>:8000`

Ensure these ports are allowed in your EC2 security group and not blocked by a corporate firewall.

---

### 5. **Recommended Practices**
- Use Elastic IP for consistent access
- Restrict SSH access to your IP
- Never open ports 22, 8000, 8080 to 0.0.0.0/0 without need
- Use IAM roles instead of hardcoded AWS credentials
