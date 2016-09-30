import boto3
import cStringIO
from config import Config
from PIL import Image
from datetime import datetime

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
bucket = s3.Bucket(Config.s3Bucket)

results = {
    'images_resized': 0,
    'start': datetime.now()
}

# Get the queue's URL
queue = sqs.create_queue(QueueName=Config.queueName)

def process():
    # Get a message from the queue
    messages = queue.receive_messages(WaitTimeSeconds=2)

    if len(messages) is 0:
        return

    message = messages[0]

    # Download the input image from S3
    input_file = cStringIO.StringIO()
    bucket.download_fileobj(message.body, input_file)

    image = Image.open(input_file)

    # Resize to 150x100
    image.thumbnail((150,100), Image.ANTIALIAS)

    # Upload the resized image to S3
    resized_file = cStringIO.StringIO()
    image.save(resized_file, 'png')

    bucket.put_object(
        Key=message.body+'-150.png',
        Body=resized_file.getvalue()
    )

    # Delete the message from the queue
    message.delete()
    results['images_resized'] += 1

    # Recurse
    process()

# Kick things off
process()

# Output the performance results
duration_seconds = (datetime.now() - results['start']).total_seconds()
print "Run time: {} seconds".format(duration_seconds)
print "Messages processed: {}".format(results['images_resized'])
print "Messages per second: {}".format(results['images_resized']/duration_seconds)
