import boto3
import cStringIO
import json
from config import Config
from PIL import Image
from datetime import datetime

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
bucket = s3.Bucket(Config.s3Bucket)

message_found = True
images_resized = 0
start_time = datetime.now()

# Get the queue's URL
queue = sqs.create_queue(QueueName=Config.queueName)

def process():
    # Get a message from the queue
    messages = queue.receive_messages(MaxNumberOfMessages=1)

    if len(messages) is 0:
        return

    message = messages[0]
    original_file_name = json.loads(message.body)['Message']

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

    # Delete the message from the queue
    message.delete()

    print "Processed {}".format(original_file_name)
    return True

# Kick things off
while message_found:
    message_found = process()

    if message_found:
        images_resized += 1

# Output the performance results
end_time = datetime.now()
print "Start time: {}".format(start_time)
print "End time: {}".format(end_time)
duration_seconds = (end_time - start_time).total_seconds()
print "Run time: {} seconds".format(duration_seconds)
print "Messages processed: {}".format(images_resized)
print "Messages per second: {}".format(images_resized/duration_seconds)
