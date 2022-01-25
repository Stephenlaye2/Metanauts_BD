import boto3

AWS_REGION = "eu-west-2"

bucket_name = "stephenlaye-boto3-bucket"

# Connetint to s3 bucket
resource = boto3.resource("s3", region_name=AWS_REGION)

object = resource.Object(bucket_name, "100-rows.csv")

# Downloading 100-rows.csv to /tmp directory
object.download_file("/tmp/100-rows.csv")

