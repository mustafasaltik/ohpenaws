import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
import sys
import shutil

# S3 client
s3_client = boto3.client("s3")

# Input and output bucket details
INPUT_BUCKET = "ohpeninput"
OUTPUT_BUCKET = "ohpenoutput"
INPUT_KEY = "transactions.csv"  # Update with the correct path to the CSV file
OUTPUT_PREFIX = "transactions"  # Prefix for the output Parquet files

# List of valid currency codes (ISO 4217 codes)
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "INR"}  # Extend as needed


def read_csv_from_s3(bucket, key):
    """Reads a CSV file from S3 into a Pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response["Body"])
    except Exception as e:
        print(f"Error reading file from S3: {e}", file=sys.stderr)
        raise


def validate_data(df):
    """Validates the DataFrame by checking for null values and valid currency codes."""
    try:
        # Check for null values
        if df.isnull().any().any():
            raise ValueError("Dataset contains null values. Please clean the data.")

        # Validate currency codes
        if not set(df["Currency"].unique()).issubset(VALID_CURRENCIES):
            raise ValueError("Dataset contains invalid currency codes.")

        # Ensure TransactionTimestamp is in datetime format
        df["TransactionTimestamp"] = pd.to_datetime(df["TransactionTimestamp"], errors="coerce")
        if df["TransactionTimestamp"].isnull().any():
            raise ValueError("Dataset contains invalid timestamps.")

        return df
    except Exception as e:
        print(f"Error validating data: {e}", file=sys.stderr)
        raise


def write_parquet_to_s3(df, bucket, prefix):
    """Writes a DataFrame to S3 in Parquet format, partitioned by year and month."""
    try:
        # Add partition columns for year and month
        df["Year"] = df["TransactionTimestamp"].dt.year
        df["Month"] = df["TransactionTimestamp"].dt.month.apply(lambda x: f"{x:02}")

        # Convert to Arrow Table
        table = pa.Table.from_pandas(df)

        # Write partitioned Parquet files locally
        output_dir = "/tmp/parquet_output"
        os.makedirs(output_dir, exist_ok=True)
        pq.write_to_dataset(
            table,
            root_path=output_dir,
            partition_cols=["Year", "Month"],
        )

        # Upload Parquet files to S3
        for root, _, files in os.walk(output_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_path = os.path.join(root, file)
                    s3_key = os.path.join(prefix, os.path.relpath(local_path, output_dir))
                    s3_client.upload_file(local_path, bucket, s3_key)


        # Clean up local files
        shutil.rmtree(output_dir)
    except Exception as e:
        print(f"Error writing Parquet files to S3: {e}", file=sys.stderr)
        raise


def main():
    try:
        # Read the data
        print("Reading data from S3...")
        df = read_csv_from_s3(INPUT_BUCKET, INPUT_KEY)

        # Validate the data
        print("Validating data...")
        df = validate_data(df)

        # Write the validated data to S3 in Parquet format
        print("Writing validated data to S3...")
        write_parquet_to_s3(df, OUTPUT_BUCKET, OUTPUT_PREFIX)

        print("Data processing complete.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
