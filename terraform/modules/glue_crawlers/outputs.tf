output "crawler_names" {
  value = [
    aws_glue_crawler.customers.name,
    aws_glue_crawler.kpi_2.name
  ]
}
