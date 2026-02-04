resource "aws_glue_job" "etl_customers" {
  name     = "ETL_Customers_v2"
  role_arn = var.glue_role_arn

  timeout = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.curated_bucket}/scripts/etl_customers.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5

  default_arguments = {
    "--RAW_BUCKET"    = var.raw_bucket
    "--OUTPUT_BUCKET" = var.curated_bucket

    # ✅ INCREMENTAL ENABLED
    "--job-bookmark-option" = "job-bookmark-enable"

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = ""
  }
}

resource "aws_glue_job" "etl_operational_health" {
  name     = "Kpi_2_v2"
  role_arn = var.glue_role_arn

  timeout = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.curated_bucket}/scripts/etl_operational_health.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5

  default_arguments = {
    "--RAW_BUCKET"    = var.raw_bucket
    "--OUTPUT_BUCKET" = var.curated_bucket

    # ✅ INCREMENTAL ENABLED
    "--job-bookmark-option" = "job-bookmark-enable"

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = ""
  }
}
