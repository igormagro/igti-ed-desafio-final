resource "aws_s3_bucket" "raw-zone" {
  bucket = "raw-zone"
  acl    = "private"
  tags   = var.tags
}

resource "aws_s3_bucket" "processing" {
  bucket = "processing-zone"
  acl    = "private"
  tags   = var.tags
}

resource "aws_s3_bucket" "consumer" {
  bucket = "consumer-zone"
  acl    = "private"
  tags   = var.tags
}