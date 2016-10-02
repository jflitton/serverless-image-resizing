import boto3
import cStringIO
import json
from config import Config
from PIL import Image
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
bucket = s3.Bucket(Config.s3Bucket)

message_found = True
images_resized = 0
start_time = datetime.now()

# Get the queue's URL
queue = sqs.create_queue(QueueName=Config.queueName)

def get_messages():
    return queue.receive_messages(MaxNumberOfMessages=10)

def delete_messages(messages):
    return queue.delete_messages(
        Entries=[{'Id': message.message_id, 'ReceiptHandle': message.receipt_handle} for message in messages]
    )

def process(message):
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

    print "Processed {}".format(original_file_name)


messages = get_messages()

while len(messages):
    print "Batch of {} started".format(len(messages))
    with ThreadPoolExecutor(max_workers=len(messages)) as tpx:
        tpx.map(
            lambda message: process(message), messages
        )

    print "Batch done!"

    images_resized += len(messages)
    delete_messages(messages)
    messages = get_messages()

# Output the performance results
end_time = datetime.now()
print "Start time: {}".format(start_time)
print "End time: {}".format(end_time)
duration_seconds = (end_time - start_time).total_seconds()
print "Run time: {} seconds".format(duration_seconds)
print "Messages processed: {}".format(images_resized)
print "Messages per second: {}".format(images_resized/duration_seconds)
