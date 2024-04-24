#!/bin/bash

# Define the file path
FILE_PATH="data.json"
STREAM_NAME="flink-test"
PARTITION_KEY="asd"
PROFILE="data-services-staging"
REGION="us-east-1"
ENDPOINT_URL="http://localhost:4567"

# Check if the file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "File not found: $FILE_PATH"
    exit 1
fi

# Read from the file line by line
while IFS= read -r line
do
    # Encode the line in base64
    encodedData=$(echo -n "$line")

    # Use AWS CLI to send the record to Kinesis
    AWS_ACCESS_KEY_ID="your_access_key" AWS_SECRET_ACCESS_KEY="your_secret_key" aws kinesis put-record  --region "$REGION" \
        --endpoint-url "$ENDPOINT_URL" \
        --stream-name "$STREAM_NAME" \
        --partition-key "$PARTITION_KEY" \
        --data "$encodedData"
        

    # Check if the AWS CLI command was successful
    if [ $? -ne 0 ]; then
        echo "Failed to send data to Kinesis: $line"
        exit 1
    fi
done < "$FILE_PATH"
