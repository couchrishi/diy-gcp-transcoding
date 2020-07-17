import os
import sys, uuid
# pylint: disable=import-error
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1, storage
# pylint: enable=import-error
from concurrent.futures import TimeoutError
from google.cloud import datastore

#UUID = uuid.uuid4().hex
PROJECT = 'saibalaji-scratchpad'
TOPIC = 'uploaded-input-assets'
topic_path = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
#SUBSCRIPTION = "worker-subscription-" + UUID
SUBSCRIPTION = "worker-subscription"
INPUT_BUCKET = "diy-transcoder-input"
OUTPUT_BUCKET = "diy-transcoder-output"

publisher_client = pubsub_v1.PublisherClient()
subscriber_client = pubsub_v1.SubscriberClient()
client = datastore.Client()

#timeout = 1.0

def updateds(asset_name,status,output_location):
    with client.transaction():
        print("I'm under Datastore")
        kind = 'Transcoding'
        name = asset_name
        task_key = client.key(kind, name)
        task = datastore.Entity(key=task_key)
        print(task)
        task.update({
            'category': 'transcoding',
            'status': status,
            'Video Name': asset_name,
            'Output Location': output_location
            })
        client.put(task)
        result = client.get(task_key)
        return(result)

def transcode(object_name):
    input_name = str(object_name,'utf-8')
    output_file_name = os.path.splitext(input_name)[0]+'.webm'
    ##print(output_file_name)

    print("I'm inside transcode")
    client = storage.Client(PROJECT)
    i_bucket = client.bucket(INPUT_BUCKET)
    o_bucket = client.bucket(OUTPUT_BUCKET)
    input_blob = i_bucket.blob(input_name)
    output_blob = o_bucket.blob(output_file_name)

    destination_file_name = '/tmp/{}'.format(input_name)
    ##print(destination_file_name)

     ## Initialize the Entity Task
    ds_result = updateds(input_name,'Initializing','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))
    input_blob.download_to_filename(destination_file_name)
    os.system('rm /tmp/{}'.format(output_file_name))
    ret = os.system('/usr/bin/avconv -i /tmp/{} -c:v libvpx -crf 10 -b:v 1M -c:a libvorbis /tmp/{}'.format(input_name,output_file_name))
    if ret:
        sys.stderr.write("FAILED")
        ds_result = updateds(input_name,'Failed','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))

    local_output_location = '/tmp/{}'.format(output_file_name)
    output_blob.upload_from_filename(local_output_location)
    print("---------------------------------------------------------------")
    ds_result = updateds(input_name,'Completed','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))

    print("Sending result to call back")
    return ds_result

if __name__ == '__main__':
    ### CREATION OF TOPIC IF NOT EXISTS
    SUBSCRIPTION = "worker-subscription"
    topic_path = publisher_client.topic_path(PROJECT, TOPIC)

    try:
        topic = publisher_client.create_topic(topic_path)
    except AlreadyExists:
        print(topic_path)
    except Exception:
        print("I'm under exception")
        print(Exception)

    ### CREATION OF SUBSCRIPTION IF NOT EXISTS

    subscription_path = subscriber_client.subscription_path(PROJECT, SUBSCRIPTION)
    try:
        subscription = subscriber_client.create_subscription(
            subscription_path, topic_path
        )
        print("Subscription created: {}".format(subscription))
    except AlreadyExists:
        print("AlreadyExists")
    except Exception:
        print(Exception)

    def callback(message):
        print(message.data)
        message.ack()
        if message:
            status = transcode(message.data)
            print("Sending result to  main")
            return status
            ##print("Ready to transcode")


    #streaming_pull_future = subscriber_client.subscribe(subscription_path, callback=callback)
    streaming_pull_future = subscriber_client.subscribe(subscription_path, callback = callback)
    print("Listening for messages on {}..\n".format(subscription_path))
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber_client:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            future_result = streaming_pull_future.result()
            print(future_result)
            
            print("final_closure")
        except TimeoutError:
            streaming_pull_future.cancel()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
    # [END pubsub_subscriber_async_pull]
    # [END pubsub_quickstart_subscriber]
