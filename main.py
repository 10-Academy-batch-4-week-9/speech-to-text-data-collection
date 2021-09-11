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
        text =  "አገራችን ከአፍሪካም ሆነ ከሌሎች የአለም አገራት ጋር ያላትን አለም አቀፋዊ ግንኙነት ወደ ላቀ ደረጃ ያሸጋገረ ሆኗል በአገር ውስጥ አራት አለም ጀልባያውም የወረቀት"
        return render_template('index.html',data=text)
    if request.method == "POST":
        f = request.files['audio_data']
        with open('audio.wav', 'wb') as audio:
            #send the file to kafka
            f.save(audio)
        print('file uploaded successfully')

        return render_template('index.html', request="POST")



if __name__ == "__main__":
    app.run(debug=True)