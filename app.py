"""
Return response as desired in UI

pip install Flask
pip install Flask-Cors
pip install boto3
pip install Werkzeug

pip freeze > requirements.txt

https://stackoverflow.com/questions/44727052/handling-large-file-uploads-with-flask

https://forums.aws.amazon.com/thread.jspa?messageID=741338
https://gist.github.com/frfahim/73c0fad6350332cef7a653bcd762f08d

sudo apt-get install tmux
tmux new
ctrl+b d
"""

import os

from flask import Flask, request, jsonify, Response, redirect, url_for
from flask_cors import CORS, cross_origin
import json

from werkzeug.utils import secure_filename

import boto3
import sys

import time
import collections

from video_extractor import VideoDetect


app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
  return 'Server Works!'


@app.route('/analyse-video', methods=['POST'])
def upload_file():
	#print request.files
	if 'file' not in request.files:
		return "No file found"

	fp = request.files['file']

	analyzer = VideoDetect()
	
	val = ""

	if fp:
		val = analyzer.upload_s3(fp)

	s3_file_name =  secure_filename(fp.filename)

	response_dict = []
	response_dict.append( {"file": s3_file_name})

	return Response(json.dumps(response_dict),  mimetype='application/json')


@app.route('/extract-labels')
def video_labels():
	# here we want to get the value of user (i.e. ?file=some-value)
	video_file_name = request.args.get('file')

	analyzer = VideoDetect()
	val = analyzer.main(video_file_name)

	response_dict = [ ]
	response_dict.append( {"object-detected": val})

	return Response(json.dumps(response_dict),  mimetype='application/json')


@app.route('/extract-face-analysis')
def video_face_labels():
	# here we want to get the value of user (i.e. ?file=some-value)
	video_file_name = request.args.get('file')

	analyzer = VideoDetect()
	val = analyzer.extractFacialFeature(video_file_name)

	response_dict = [ ]
	response_dict.append( {"face-analysis": val})

	return Response(json.dumps(response_dict),  mimetype='application/json')






