#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask,url_for
from flask import request
from flask import render_template

import os

app = Flask(__name__)


@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == 'GET':
        # randomly select text and send to user
        text =  "የተሻለ ብቃት ያሳዩ ቦክሰኞች ለቶኪዮ ኦሊምፒክ ማጣሪያ ተሳታፊ እንደሚሆኑም ታውቋል"
        return render_template('index.html',data=text)
    if request.method == "POST":
        f = request.files['audio_data']
        with open('audio.wav', 'wb') as audio:
            #send the file to kafka
            f.save(audio)
        print('file uploaded successfully')

        return render_template('index.html', request="POST")
@app.route("/app2", methods=['POST', 'GET'])
def app2():
    if request.method == 'GET':
        # randomly select text and send to user
        text =  "የተሻለ ብቃት ያሳዩ ቦክሰኞች ለቶኪዮ ኦሊምፒክ ማጣሪያ ተሳታፊ እንደሚሆኑም ታውቋል"
        return render_template('index2.html',data=text)
    if request.method == "POST":
        f = request.files['audio_data']
        with open('audio.wav', 'wb') as audio:
            #send the file to kafka
            f.save(audio)
        print('file uploaded successfully')

        return render_template('index2.html', request="POST")



if __name__ == "__main__":
    app.run(debug=True)