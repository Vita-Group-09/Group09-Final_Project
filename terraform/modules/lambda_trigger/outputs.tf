output "lambda_arn" {
  value = aws_lambda_function.s3_raw_trigger.arn
}

output "lambda_name" {
  value = aws_lambda_function.s3_raw_trigger.function_name
}
