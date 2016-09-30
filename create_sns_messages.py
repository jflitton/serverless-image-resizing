import boto3
from config import Config

sns = boto3.resource('sns')
topic = sns.create_topic(Name=Config.snsTopic)

for i in range(1, 11):
    for j in range(1,101):
        image_name = "{}-sloth{}.jpg".format(j, i)
        print "Creating message {}".format(image_name)

        topic.publish(
            Message="{}/{}".format(Config.s3Folder, image_name)
        )
