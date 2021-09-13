#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask,url_for
from flask import request, redirect
from flask import render_template
from kafka import KafkaConsumer, KafkaProducer


import os

app = Flask(__name__)


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
        b = f.read()

        print(type(f))
        print('---------')
        print(b)
        print('---------')
        print('file uploaded successfully')
        with open('audio.wav', 'wb') as audio:
            #send the file to kafka
            f.save(audio)

        return redirect(url_for("index"))
        # return render_template('index.html', data=next(text_gen), request="GET")
    else:
        # if request.method == 'GET':
        # randomly select text and send to user
        # text =  "የተሻለ ብቃት ያሳዩ ቦክሰኞች ለቶኪዮ ኦሊምፒክ ማጣሪያ ተሳታፊ እንደሚሆኑም ታውቋል"
        return render_template('index.html',data=next(text_gen))


if __name__ == "__main__":
    app.run(debug=True, port=5002)