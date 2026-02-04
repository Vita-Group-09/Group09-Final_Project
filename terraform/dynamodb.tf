# 🔒 Runtime execution lock for Step Function pipeline
resource "aws_dynamodb_table" "pipeline_lock" {
  name         = "airport-intelligence-pipeline-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_name"

  attribute {
    name = "pipeline_name"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
