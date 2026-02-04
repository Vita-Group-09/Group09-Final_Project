data "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

module "curated_s3" {
  source      = "./modules/s3"
  bucket_name = var.curated_bucket_name
}

module "glue_database" {
  source        = "./modules/glue"
  database_name = var.glue_database_name
}

module "glue_iam_role" {
  source         = "./modules/iam"
  raw_bucket     = var.raw_bucket_name
  curated_bucket = var.curated_bucket_name
}

module "glue_jobs" {
  source         = "./modules/glue_jobs"
  raw_bucket     = var.raw_bucket_name
  curated_bucket = var.curated_bucket_name
  glue_role_arn  = module.glue_iam_role.glue_role_arn
}

module "glue_crawlers" {
  source         = "./modules/glue_crawlers"
  curated_bucket = var.curated_bucket_name
  glue_database  = var.glue_database_name
  glue_role_arn  = module.glue_iam_role.glue_role_arn
}

# 🔥 STEP FUNCTIONS ORCHESTRATION (CLEAN, NO CONGESTION)
module "orchestration" {
  source = "./modules/orchestration"
}
module "lambda_trigger" {
  source            = "./modules/lambda_trigger"
  step_function_arn = module.orchestration.state_machine_arn
}

resource "aws_lambda_permission" "allow_raw_s3" {
  statement_id  = "AllowRawS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_trigger.lambda_name
  principal     = "s3.amazonaws.com"
  source_arn    = data.aws_s3_bucket.raw.arn
}

resource "aws_s3_bucket_notification" "raw_trigger" {
  bucket = data.aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = module.lambda_trigger.lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }

  depends_on = [aws_lambda_permission.allow_raw_s3]
}
