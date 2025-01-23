import unittest
from unittest.mock import patch
import pandas as pd
import boto3
import io
from moto import mock_aws

# Import functions from the script
from main import read_csv_from_s3, validate_data, write_parquet_to_s3


class TestS3ValidationScript(unittest.TestCase):
    @mock_aws
    def setUp(self):
        # Sample CSV data
        self.csv_data = """TransactionID,CustomerID,TransactionAmount,Currency,TransactionTimestamp
            1,123,100.50,USD,2024-01-15 10:30:00
            2,456,-50.75,EUR,2024-02-20 15:45:00
            3,789,75.25,GBP,2024-03-25 20:15:00
            """

        # Output bucket
        self.output_bucket = "ohpenoutput"

    @mock_aws
    def test_read_csv_from_s3(self):
        # Create a mocked S3 client
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        key = "test.csv"
        content = "TransactionID,CustomerID,TransactionAmount,Currency,TransactionTimestamp\n1,123,100.0,USD,2024-01-01"

        # Create bucket and upload a mock CSV file
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)

        # Test the function
        df = read_csv_from_s3(bucket_name, key)
        assert not df.empty
        assert df.columns.tolist() == [
            "TransactionID",
            "CustomerID",
            "TransactionAmount",
            "Currency",
            "TransactionTimestamp",
        ]

    def test_validate_data(self):
        """Test data validation."""
        df = pd.read_csv(io.StringIO(self.csv_data))
        validated_df = validate_data(df)

        # Check if all rows pass validation
        self.assertEqual(len(validated_df), 3)

    @mock_aws
    @patch("main.os.makedirs")
    @patch("main.shutil.rmtree")
    def test_write_parquet_to_s3(self, mock_rmtree, mock_makedirs):
        """Test writing Parquet data to S3."""
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "ohpenoutput"
        s3.create_bucket(Bucket=bucket_name)

        df = pd.read_csv(io.StringIO(self.csv_data))
        validated_df = validate_data(df)

        write_parquet_to_s3(validated_df, bucket_name, "transactions")

        # Check if files are written to S3
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix="transactions")
        self.assertGreater(result["KeyCount"], 0)

    @mock_aws
    def test_invalid_currency(self):
        """Test that invalid currency codes raise an error."""
        invalid_data = """TransactionID,CustomerID,TransactionAmount,Currency,TransactionTimestamp
        1,123,100.50,XXX,2024-01-15 10:30:00
        """
        df = pd.read_csv(io.StringIO(invalid_data))
        with self.assertRaises(ValueError):
            validate_data(df)

    @mock_aws
    def test_null_values(self):
        """Test that null values raise an error."""
        null_data = """TransactionID,CustomerID,TransactionAmount,Currency,TransactionTimestamp
        1,123,100.50,USD,
        """
        df = pd.read_csv(io.StringIO(null_data))
        with self.assertRaises(ValueError):
            validate_data(df)

    @mock_aws
    def test_invalid_timestamp(self):
        """Test that invalid timestamps raise an error."""
        invalid_data = """TransactionID,CustomerID,TransactionAmount,Currency,TransactionTimestamp
        1,123,100.50,USD,InvalidDate
        """
        df = pd.read_csv(io.StringIO(invalid_data))
        with self.assertRaises(ValueError):
            validate_data(df)


if __name__ == "__main__":
    unittest.main()
