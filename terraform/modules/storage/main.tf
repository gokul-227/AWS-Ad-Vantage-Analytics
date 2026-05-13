# DynamoDB table for real-time metrics
resource "aws_dynamodb_table" "metrics" {
  name         = "${var.project}-${var.environment}-metrics"
  billing_mode = "PAY_PER_REQUEST"  # Free tier friendly
  hash_key     = "metricKey"
  range_key    = "timestamp"

  attribute {
    name = "metricKey"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "N"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = { Name = "${var.project}-metrics-table" }
}

resource "random_id" "suffix" {
  byte_length = 4
}
