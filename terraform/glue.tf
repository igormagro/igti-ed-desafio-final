# resource "aws_glue_crawler" "gold-layer-catalog" {
#   database_name = "DW-IGTI"
#   name          = "gold-layer-catalog"
#   role          = "arn:aws:iam::612155163873:role/glue_role"

#   s3_target {
#     path = "s3://imb-gold-layer"
#   }
# }

# resource "aws_iam_role" "glue_role" {
#   name = "glue_role"

#   # Terraform's "jsonencode" function converts a
#   # Terraform expression result to valid JSON syntax.
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Action = "sts:AssumeRole"
#         Effect = "Allow"
#         Sid    = ""
#         Principal = {
#           Service = "ec2.amazonaws.com"
#         }
#       },
#     ]
#   })

#   tags = {
#     tag-key = "tag-value"
#   }
# }