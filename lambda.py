import boto3
import cStringIO
from config import Config
from PIL import Image

s3 = boto3.resource('s3')
bucket = s3.Bucket(Config.s3Bucket)

def resize(event, context):
    original_file_name = event['Records'][0]['Sns']['Message']

    # Download the input image from S3
    input_file = cStringIO.StringIO()
    bucket.download_fileobj(original_file_name, input_file)

    image = Image.open(input_file)

    # Resize to 150x100
    image.thumbnail((150,100), Image.ANTIALIAS)

    # Upload the resized image to S3
    resized_file = cStringIO.StringIO()
    image.save(resized_file, 'png')

    bucket.put_object(
        Key=original_file_name + '-150.png',
        Body=resized_file.getvalue()
    )