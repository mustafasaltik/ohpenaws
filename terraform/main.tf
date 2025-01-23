provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "input_bucket" {
  bucket = "ohpeninput"
}

resource "aws_s3_bucket" "output_bucket" {
  bucket = "ohpenoutput"
}

resource "aws_iam_role" "s3_access_role" {
  name = "S3AccessRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}
