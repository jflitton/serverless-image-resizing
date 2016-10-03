import boto3
from config import Config
from concurrent.futures import ThreadPoolExecutor

sns = boto3.resource('sns')
topic = sns.create_topic(Name=Config.snsTopic)

image_names = []

def publish(image_name):
    topic.publish(
        Message="{}/{}".format(Config.s3Folder, image_name)
    )

for i in range(1, 11):
    for j in range(1,101):
        image_names.append("{}-sloth{}.jpg".format(j, i))

with ThreadPoolExecutor(max_workers=100) as tpx:
    tpx.map(
        lambda image_name: publish(image_name), image_names
    )

