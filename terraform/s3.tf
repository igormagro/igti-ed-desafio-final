resource "aws_s3_bucket" "bronze" {
  bucket = "imb-bronze-layer"
  acl    = "private"
  tags   = var.tags
}

resource "aws_s3_bucket" "silver" {
  bucket = "imb-silver-layer"
  acl    = "private"
  tags   = var.tags
}

resource "aws_s3_bucket" "gold" {
  bucket = "imb-gold-layer"
  acl    = "private"
  tags   = var.tags
}

