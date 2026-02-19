# 🚀 AWS Serverless ETL CI/CD Pipeline — Lambda + Glue + Crawlers (Event Driven)

## 📌 Overview

This project implements a fully automated **serverless ETL pipeline with CI/CD** using:

- AWS Lambda (event-driven orchestration)
- AWS Glue ETL Jobs
- AWS Glue Crawlers
- AWS CloudFormation (Infrastructure as Code)
- GitHub Actions (CI/CD)
- Amazon S3 Data Lake
- Amazon Athena

The pipeline is **event-driven, automated, failure-aware, and production-style**. Everything is deployed via CI/CD — no manual AWS Console setup required.

---

# 🏗️ Final End-to-End Architecture Flow

```
EC2 Ingestion
    ↓
S3 RAW Bucket (new file arrives)
    ↓  [S3 ObjectCreated Event]
Lambda Trigger (Orchestrator)
    ↓
Start Glue Job
    ↓
Wait till SUCCEEDED
    ↓
Gold Data Written to S3
    ↓
Lambda starts Crawlers
    ↓
Airline Crawler → SUCCESS
    ↓
Customers Crawler → SUCCESS
    ↓
Glue Data Catalog Updated
    ↓
Athena Tables Ready
```

---

# 📁 Repository Structure (Final)

```
.
├── .github/workflows/
│   └── ci.yml

├── glue_job.py
├── lambda.py

├── glue-template.yml
├── crawler-template.yml
├── lambda-template.yml

└── README.md

---

# ⚙️ CI/CD Trigger Rules

Pipeline runs on:

- Push to `develop` branch
- Manual run from GitHub Actions (workflow_dispatch)

CI/CD deploys **infrastructure + code only**. Execution is event-driven via Lambda after RAW S3 upload.

---

# 🔄 CI/CD Deployment Steps (GitHub Actions)

1. Checkout repository
2. Configure AWS credentials
3. Upload `glue_job.py` to S3
4. Zip and upload `lambda.py` to S3
5. Deploy Lambda stack (CloudFormation)
6. Deploy Glue Job stack (CloudFormation)
7. Deploy Crawlers stack (CloudFormation)
8. Validate stack deployment
9. Mark CI/CD success

Glue job is **not started from CI** — Lambda controls runtime execution.

---

# 🧠 Infrastructure as Code — 3 CloudFormation Stacks

## ✅ Lambda Stack (lambda-template.yml)

Creates:

- Lambda function (orchestrator)
- Lambda IAM role
- S3 RAW bucket trigger
- Environment variables:
  - Glue job name
  - Airline crawler name
  - Customers crawler name

Purpose:

- Event-driven orchestration
- Controls full pipeline execution order

---

## ✅ Glue Job Stack (glue-template.yml)

Creates:

- Glue Job
- Glue IAM role
- Worker config (G.1X — 10 workers)
- Script S3 location
- TempDir auto path

Configured with:

- GlueVersion 4.0
- Python 3
- Metrics enabled
- Continuous logging enabled
- MaxConcurrentRuns = 1
- Job bookmarks disabled

Purpose:

- Transform RAW → GOLD
- Produce customers + airline gold datasets

---

## ✅ Crawler Stack (crawler-template.yml)

Creates:

- Glue Database
- Airline crawler
- Customers crawler
- IAM role
- Gold S3 scan targets

Targets:

```
gold/airline/
gold/customers/
```

Purpose:

- Schema detection
- Glue Data Catalog update
- Athena-ready tables

---

# ⚡ Lambda Orchestration Logic

Lambda is the **pipeline controller**.

Trigger:

- S3 RAW bucket → ObjectCreated event

Execution steps:

1. Receive S3 event
2. Start Glue job
3. Capture JobRunId
4. Poll Glue job status
5. Wait until SUCCEEDED
6. Start airline crawler
7. Wait until READY + SUCCEEDED
8. Start customers crawler
9. Wait until READY + SUCCEEDED
10. Exit success

If any step fails → Lambda raises error → pipeline stops immediately.

---

# 🔁 Glue Job Execution Logic

Glue job performs:

- Raw CSV ingestion
- Type casting and cleaning
- Delay & KPI aggregations
- Lookup joins (airport + carrier)
- Metric engineering
- Writes TWO gold outputs:

```
s3://airport-airline-operations-analytics-platform/gold/customers/
s3://airport-airline-operations-analytics-platform/gold/airline/
```

No silver storage — direct gold generation.

---

# 🕷️ Crawler Strategy (Sequential Only)

Crawlers run **strictly sequentially** — never parallel.

Execution order:

1️⃣ airline crawler  
2️⃣ customers crawler  

For each crawler:

- Start crawler
- Poll crawler state
- Wait until READY
- Validate LastCrawl status
- Fail pipeline if status ≠ SUCCEEDED

Prevents catalog race conditions and schema conflicts.

---

# 🪣 S3 Folder Auto-Creation

No folders must exist beforehand.

Auto-created by:

- Glue write operations
- Lambda-trigger flow
- Glue TempDir config
- Crawler scan targets

Zero manual bucket folder setup required.

---

# 🔐 Required GitHub Secrets

Set in repository secrets:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
```

---

# 🔑 Required IAM Permissions (CI/CD User)

CI/CD IAM user must allow:

- CloudFormation deploy/update/delete
- Glue full control
- Lambda create/update
- S3 object write
- iam:PassRole
- iam:PutRolePolicy
- iam:AttachRolePolicy

If missing → stack creation fails.

---

# ❌ Automatic Failure Conditions

Pipeline fails automatically if:

- Lambda fails
- Glue job fails / stops / times out
- Crawler fails
- CloudFormation stack fails
- Script upload fails
- IAM access denied

No partial success allowed.

---

# 📊 Final Output

After successful execution:

- Gold datasets generated
- Glue tables created
- Data Catalog updated
- Athena query-ready tables
- Fully automated event-driven ETL pipeline completed

---

# 🎯 Use Cases

- Production ETL automation
- Event-driven data lake pipelines
- Serverless data engineering demos
- Glue CI/CD portfolio projects
- IaC data workflows
- Interview & presentation demos

---

# ✅ Result

This project demonstrates:

- Event-driven ETL orchestration
- CI/CD for data pipelines
- Lambda-controlled execution
- Glue + Crawler automation
- Infrastructure as Code
- Sequential execution safety
- Failure-aware orchestration
- Production-grade AWS serverless data pipeline
- [Dashboard](https://app.powerbi.com/links/gY80OZnVHO?ctid=ca456806-67ee-4b5f-8305-f1d18a9bc96e&pbi_source=linkShare)


