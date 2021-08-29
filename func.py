import io
import json
from fdk import response
from kafka import KafkaConsumer, TopicPartition
import func
from minio import Minio
from minio.error import InvalidResponseError
import os
import logging
logger = logging.getLogger()
logger.disabled = True
import urllib3
import time

def handler(ctx, data: io.BytesIO = None):
    # Default Topic
    topic = "default"
    try:
        body = json.loads(data.getvalue())
        topic = body.get("topic")
        # Main function for our-use case. Connect to Kafka , read , Create a file and upload it to our local clusterm
        readingKafkaandUpload(topic)
    finally:
        logging.getLogger().info('changed error parsing json payload: ')

    logging.getLogger().info("Inside Python Hello World function")
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Hello {0}".format(topic)}),
        headers={"Content-Type": "application/json"}
    )



def readingKafkaandUpload(topic):
    connect_str="localhost:29092"
    group_id='group0'
    consumers = KafkaConsumer(topic,bootstrap_servers=connect_str,group_id=group_id,consumer_timeout_ms=2000,heartbeat_interval_ms=500)
    consumers.poll()
    consumers.seek_to_beginning()
    file= topic + "-mycsvdata-" + time.strftime("%Y%m%d-%H%M%S") + ".csv" 
    print(file)
    try:
        with open(file, 'a') as file_data:
            for message in consumers:
                print(message.value)
                if message.value : 
                    file_data.writelines(message.value.decode('utf-8') + os.linesep)
                # consumers.commit_async()
    finally:
            print("Reading from Kafka Topic Over")
    print("here")
    if (os.path.getsize(file) > 0) : 
        uploadtoMinio(file,topic)
    consumers.close()
    return "success"

def uploadtoMinio(file,bucket):
    client = Minio('localhost:9001',
                access_key='admin',
                secret_key='password',secure=False,)

    bucket = bucket.lower()
    if not client.bucket_exists(bucket):
        print(bucket)
        client.make_bucket(bucket)
    try:
        with open(file, 'rb') as file_data:
            file_stat = os.stat(file)
            client.put_object(bucket, file,
                            file_data, file_stat.st_size)
    except InvalidResponseError as err:
        print(err)

def tearDown(topic):
    bucket = topic.lower()
    try:
        client = Minio('localhost:9000',
                access_key='admin',
                secret_key='password',secure=False,)

        if client.bucket_exists(bucket):
            # List objects information.
            objects = client.list_objects(bucket)
            for obj in objects:
                print(obj.bucket_name)
                client.remove_object(obj.bucket_name,obj.object_name)
                # print(client.remove_object(obj.object_name,obj.bucket_name))

            client.remove_bucket(bucket)
    except InvalidResponseError as err:
        print(err)

def execute(topic):
    try:
        readingKafkaandUpload(topic)
    finally:
        print("done")
