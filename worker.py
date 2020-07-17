import os
import sys
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1, storage
from concurrent.futures import TimeoutError
from google.cloud import datastore

PROJECT = '<GCP PROJECT NAME>'
TOPIC = 'PUBSUB TOPIC NAME'
topic_path = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
SUBSCRIPTION = 'PUBSUB SUBSCRIPTION NAME'
INPUT_BUCKET = "GCS INPUT BUCKET NAME"
OUTPUT_BUCKET = "GCS OUTPUT BUCKET NAME"

publisher_client = pubsub_v1.PublisherClient()
subscriber_client = pubsub_v1.SubscriberClient()
client = datastore.Client()

def updateds(asset_name,status,output_location): nh 
    with client.transaction():
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
    client = storage.Client(PROJECT)
    i_bucket = client.bucket(INPUT_BUCKET)
    o_bucket = client.bucket(OUTPUT_BUCKET)
    input_blob = i_bucket.blob(input_name)
    output_blob = o_bucket.blob(output_file_name)

    destination_file_name = '/tmp/{}'.format(input_name)
    ds_result = updateds(input_name,'Initializing','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))
    input_blob.download_to_filename(destination_file_name)
    os.system('rm /tmp/{}'.format(output_file_name))
    ret = os.system('/usr/bin/avconv -i /tmp/{} -c:v libvpx -crf 10 -b:v 1M -c:a libvorbis /tmp/{}'.format(input_name,output_file_name))
    if ret:
        sys.stderr.write("FAILED")
        ds_result = updateds(input_name,'Failed','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))

    local_output_location = '/tmp/{}'.format(output_file_name)
    output_blob.upload_from_filename(local_output_location)
    ds_result = updateds(input_name,'Completed','https://storage.googleapis.com/{}/{}'.format(OUTPUT_BUCKET,output_file_name))
    return ds_result

if __name__ == '__main__':

    SUBSCRIPTION = 'PUBSUB SUBSCRIPTION NAME'
    topic_path = publisher_client.topic_path(PROJECT, TOPIC)
    
    ### CREATION OF TOPIC IF NOT EXISTS
    try:
        topic = publisher_client.create_topic(topic_path)
    except AlreadyExists:
        print(topic_path)
    except Exception:
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


    streaming_pull_future = subscriber_client.subscribe(subscription_path, callback = callback)
    print("Listening for messages on {}..\n".format(subscription_path))

    with subscriber_client:
        try:
            future_result = streaming_pull_future.result()
            print(future_result)
            
            print("final_closure")
        except TimeoutError:
            streaming_pull_future.cancel()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()