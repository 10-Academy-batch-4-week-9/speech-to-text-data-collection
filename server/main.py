#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask,url_for
from flask import request, redirect
from flask import render_template
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
import json
import boto3
import logging
import hashlib
import datetime;
from botocore.exceptions import ClientError
import io
import soundfile as sf


import os

app = Flask(__name__)

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def my_gen():
    consumer = KafkaConsumer('group4_text_corpus',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-groupqp09',
                         bootstrap_servers=['b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092'])

    for message in consumer:
        yield message.value.decode()

text_gen= my_gen()


@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == "POST":
        f = request.files['audio_data']
        transcription = request.form.get('transcription')
        print(transcription)
        hash_object = hashlib.md5(transcription.encode())
        filename = hash_object.hexdigest()
        ct = datetime.datetime.now()
        ts = ct.timestamp()
        filename = f'{filename}-{ts}'
        print(filename)

        with open(f'{filename}.wav', 'wb') as audio:
            #send the file to kafka
            f.save(audio) 

        print('file saved successfully')

        if upload_file(f'{filename}.wav', 'group4-audio-bucket'):
            print('file uploaded successfully to s3')
            producer = KafkaProducer(bootstrap_servers=['b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092'], 
            value_serializer=lambda m: json.dumps(m).encode('utf-8'))

            data = {'filename': filename, 'transcription': transcription}
            future = producer.send('group4_audio', data)

            # Block for 'synchronous' sends
            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                # Decide what to do if produce request failed...
                log.exception()
                pass

        return redirect(url_for("index"))
    
    else:
        return render_template('index.html',data=next(text_gen))


if __name__ == "__main__":
    app.run(debug=True, port=5012)
