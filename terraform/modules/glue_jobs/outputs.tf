output "job_names" {
  value = [
    aws_glue_job.etl_customers.name,
    aws_glue_job.etl_operational_health.name
  ]
}
