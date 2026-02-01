# 🚀 AWS Glue CI/CD Pipeline — Automated ETL Deployment with GitHub Actions

## 📌 Overview

This project implements a fully automated **CI/CD pipeline for AWS Glue ETL workflows** using **GitHub Actions**. It automates script deployment, infrastructure provisioning, job execution, and crawler validation using Infrastructure as Code and SDK-based orchestration.

Technologies used:

* AWS Glue
* AWS CloudFormation (IaC)
* Python boto3 SDK
* GitHub Actions
* Amazon S3
* Glue Crawlers
* Amazon Athena
* (Optional) AWS Lambda orchestration

The pipeline is designed to be deterministic, failure-aware, and production-style.

---

## 🏗️ End-to-End Flow

GitHub Push (main branch)
→ GitHub Actions CI/CD
→ Upload Glue Script to S3
→ Deploy Glue Job (CloudFormation)
→ Start Glue Job
→ Wait for SUCCESS
→ Run Airline Crawler
→ Validate Crawler
→ Run Customers Crawler
→ Validate Crawler
→ Glue Data Catalog Updated
→ Athena Tables Ready

---

## 📁 Repository Structure

```
.
├── .github/workflows/
│   └── ci.yml

├── glue_job.py
├── template.yaml
├── crawler-template.yaml
├── deployment.py
├── lambda_function.py
└── README.md
```

---

## ⚙️ CI/CD Trigger Rules

Pipeline runs on:

* Push to main branch
* Pull request to main branch
* Manual run from Actions tab

---

## 🔄 CI/CD Pipeline Steps

1. Checkout repository
2. Configure AWS credentials
3. Upload Glue script to S3
4. Deploy / update Glue Job via CloudFormation
5. Start Glue Job run
6. Poll job status until **SUCCEEDED**
7. Run Airline crawler
8. Wait and validate Airline crawler
9. Run Customers crawler
10. Wait and validate Customers crawler
11. Mark pipeline success

Pipeline stops immediately if any step fails.

---

## 🧪 Glue Job Execution Logic

* Glue Job triggered programmatically
* Job run ID captured
* Status polled at intervals
* Pipeline fails on:

  * FAILED
  * STOPPED
  * TIMEOUT
* Prevents silent failures
* Avoids overlapping runs

---

## 🗂️ Glue Crawler Strategy

Crawlers run **sequentially**, not in parallel.

Execution order:

1. airline crawler
2. customers crawler

For each crawler:

* Start crawler
* Wait until state = READY
* Check LastCrawl status
* Fail pipeline if status ≠ SUCCEEDED

This prevents catalog conflicts and race conditions.

---

## 🗃️ Data Lake Scan Paths

```
s3://airport-airline-operations-analytics-platform/silver/airline/
s3://airport-airline-operations-analytics-platform/silver/customers/
```

Crawlers scan these paths and update Glue Data Catalog tables.

---

## 🧠 Infrastructure as Code

### CloudFormation Templates

**template.yaml**

* Creates / updates Glue Job
* Defines script location
* Configures IAM role and job parameters

**crawler-template.yaml**

* Creates Glue Crawlers
* Defines S3 targets and databases

Benefits:

* Repeatable deployment
* Version controlled
* Environment portable

---

### Python SDK Deployment

`deployment.py` uses boto3 to provision:

* Glue jobs
* Crawlers
* Supporting resources

Useful for dynamic or conditional deployments.

---

## 🧩 Optional Lambda Orchestration

Lambda function can orchestrate:

* Glue job execution
* Job status wait
* Sequential crawler runs

Supports event-driven pipelines (for example, S3 upload trigger).

---

## 🔐 Security Requirements

GitHub repository secrets required:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
```

IAM user permissions required:

* Glue job control
* Glue crawler control
* S3 object write
* CloudFormation deploy
* Lambda update (if used)

Use least-privilege policies in production.

---

## ❌ Automatic Failure Conditions

Pipeline fails automatically if:

* Glue job fails
* Glue job stops
* Glue job times out
* Any crawler fails
* Script upload fails
* CloudFormation deploy fails
* Lambda update fails

No partial-success states allowed.

---

## 📊 Final Output

After successful run:

* Glue job completed
* Crawlers completed sequentially
* Glue Data Catalog updated
* Athena tables available for query

---

## 🎯 Use Cases

* Data engineering CI/CD
* Glue ETL automation
* Serverless data pipeline deployment
* Data lake catalog refresh
* Production-style AWS workflows

---

## ✅ Result

This project demonstrates:

* CI/CD for data pipelines
* Automated Glue orchestration
* Infrastructure as Code
* Sequential execution control
* Failure-safe deployment
* Production-grade AWS data engineering pipeline
