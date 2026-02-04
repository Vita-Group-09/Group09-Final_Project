resource "aws_glue_crawler" "customers" {
  name          = "Customers_table_crawler_v2"
  role          = var.glue_role_arn
  database_name = var.glue_database

  s3_target {
    path = "s3://${var.curated_bucket}/transformed/customers/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_glue_crawler" "kpi_2" {
  name          = "Kpi_2_Crawler_v2"
  role          = var.glue_role_arn
  database_name = var.glue_database

  s3_target {
    path = "s3://${var.curated_bucket}/gold/airline_airport_operational_health/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}
