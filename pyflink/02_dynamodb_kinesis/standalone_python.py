import boto3

# Initialize a boto3 session
# Note: AWS credentials and region should be configured in your environment or through the AWS CLI
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Specify the table name
table_name = 'WordCounts'

# Get the table resource
table = dynamodb.Table(table_name)

# Define the item to add to the table
item = {
    'word': 'MyUniqueWord',  # Primary key
    'count': 30,
}

# Put the item into the table
try:
    response = table.put_item(Item=item)
    print(f"Successfully added item: {item}")
    print(f"AWS Response: {response}")
except Exception as e:
    print(f"Error adding item to DynamoDB table {table_name}: {e}")
