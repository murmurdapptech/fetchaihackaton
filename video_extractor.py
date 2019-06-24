#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 23:30:41 2019

@author: abhishekdutt
"""

import os
import boto3
import json
import sys
import time
from time import sleep

from flask import Flask, request, jsonify, Response, redirect, url_for
from flask_cors import CORS, cross_origin

from werkzeug.utils import secure_filename

from botocore.exceptions import NoCredentialsError, ClientError
from boto3.s3.transfer import TransferConfig

"""
@TODO:
Socket based solution
Streaming data?
# As as of now not handling for CONCURRENT different video processing using Queue
"""
class VideoDetect:
    jobId = ''

    ACCESS_KEY = ''
    SECRET_KEY = ''
    REGION = 'us-east-1'

    rek = boto3.client('rekognition',
                       aws_access_key_id = ACCESS_KEY,
                       aws_secret_access_key = SECRET_KEY,
                       region_name=REGION
                       )

    s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, 
                      region_name=REGION)
    
    queueUrl = 'https://sqs.us-east-1.amazonaws.com/402801094862/video'
    roleArn = 'arn:aws:iam::402801094862:role/rekognition_service_role_sns'
    topicArn = 'arn:aws:sns:us-east-1:402801094862:AmazonRekognitionVideoTopic'
    bucket = 'samplevideoramesh'
    video = 'Leave_Airbnb.mp4' #PGtipsGreenTea.mp4

    dynamodbPersistQueueUrl = 'https://sqs.us-east-1.amazonaws.com/402801094862/video_extract_dynamoDB'

    def upload_s3(self, local_file):
        # Set the desired multipart threshold value (5GB)
        GB = 1024 ** 3
        config = TransferConfig(multipart_threshold=5*GB)

        try:
            s3_file_name =  secure_filename(local_file.filename)
            #print(type(s3_file_name))
            self.s3.upload_fileobj(local_file, self.bucket, s3_file_name, Config=config)
            return s3_file_name+" Video File Upload Successfully !"

        except FileNotFoundError:
            return "File Not Found Error"
        except NoCredentialsError:
            return "Credentials not available"


    def main(self, video_file_name):

        jobFound = False
        sqs = boto3.client('sqs',
                           aws_access_key_id = self.ACCESS_KEY,
                           aws_secret_access_key = self.SECRET_KEY,
                           region_name = self.REGION)
       

        response = self.rek.start_label_detection(Video={'S3Object': {'Bucket': self.bucket, 'Name': video_file_name}},
                                         NotificationChannel={'RoleArn': self.roleArn, 'SNSTopicArn': self.topicArn})
        
        
        #=====================================
        #print('Start Job Id: ' + response['JobId'])
        
        jsonData = ""

        while jobFound == False:
            sqsResponse = sqs.receive_message(QueueUrl=self.queueUrl, MessageAttributeNames=['ALL'],
                                          MaxNumberOfMessages=10)

            if sqsResponse:
                # POLLING
                if 'Messages' not in sqsResponse:    
                    sys.stdout.flush()
                    continue

                for message in sqsResponse['Messages']:
                    notification = json.loads(message['Body'])
                    rekMessage = json.loads(notification['Message'])
                    if str(rekMessage['JobId']) == response['JobId']:
                        jobFound = True
                        #=============================================
                        jsonData = self.GetResultsLabels(rekMessage['JobId'])
                        #=============================================

                        sqs.delete_message(QueueUrl=self.queueUrl,
                                       ReceiptHandle=message['ReceiptHandle'])
                    
                    # Delete the unknown message. Consider sending to dead letter queue
                    # As as of now not handling for concurrent different video processing using Queue
                    sqs.delete_message(QueueUrl=self.queueUrl,
                                   ReceiptHandle=message['ReceiptHandle'])

        #print('Label Done')
        return jsonData


    def extractFacialFeature(self, video_file_name):
        jobFound = False
        sqs = boto3.client('sqs',
                           aws_access_key_id='',
                           aws_secret_access_key='',
                           region_name='us-east-1')
        
        response = self.rek.start_face_detection(FaceAttributes="ALL",Video={'S3Object':{'Bucket':self.bucket,'Name':video_file_name}},
            NotificationChannel={'RoleArn':self.roleArn, 'SNSTopicArn':self.topicArn}) 

        jsonData = ""
        
        while jobFound == False:
            sqsResponse = sqs.receive_message(QueueUrl=self.queueUrl, MessageAttributeNames=['ALL'],
                                          MaxNumberOfMessages=10)

            if sqsResponse:
                if 'Messages' not in sqsResponse:  
                    sys.stdout.flush()
                    continue

                for message in sqsResponse['Messages']:
                    notification = json.loads(message['Body'])
                    rekMessage = json.loads(notification['Message'])
                    if str(rekMessage['JobId']) == response['JobId']:
                        #print('Matching Job Found:' + rekMessage['JobId'])
                        jobFound = True
                        #=============================================
                        jsonData = self.GetResultsFaces(rekMessage['JobId'])
                        #=============================================

                        sqs.delete_message(QueueUrl=self.queueUrl,
                                       ReceiptHandle=message['ReceiptHandle'])

                    sqs.delete_message(QueueUrl=self.queueUrl,
                                   ReceiptHandle=message['ReceiptHandle'])

        return jsonData
        

    def GetResultsLabels(self, jobId):
        maxResults = 100
        paginationToken = '' # PAGINATION
        finished = False
        jsonData = {"labels": []}

        sqs = boto3.client('sqs',
                           aws_access_key_id = self.ACCESS_KEY,
                           aws_secret_access_key = self.SECRET_KEY,
                           region_name = self.REGION)
        

        while finished == False:
            
            print("Hitting GET LABEL Detection")
            response = self.rek.get_label_detection(JobId=jobId,
                                            MaxResults=maxResults,
                                            NextToken=paginationToken,
                                            SortBy='TIMESTAMP')

            print("GET LABEL Detection Successfull")


            for labelDetection in response['Labels']:
                labelData = {}
                
                label=labelDetection['Label']
                
                labelData["name"] = label['Name']
                labelData["confidence"] = label['Confidence']
                labelData["timestamp"] = labelDetection['Timestamp']
                
                jsonData["labels"].append(labelData)

                if 'NextToken' in response:
                    paginationToken = response['NextToken']
                else:
                    finished = True

        # Message Size limit is 256 KB
        # Use Job-ID, FIle Name - Lable vs Face Type too !
        # try catch logging
        # sqsResponse = sqs.send_message(QueueUrl=self.dynamodbPersistQueueUrl, MessageBody= json.dumps(jsonData))

        return jsonData
            
    
    def GetResultsFaces(self, jobId):
        maxResults = 100
        paginationToken = '' # PAGINATION
        finished = False
        jsonData = {"labels": []}

        RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException', 'ThrottlingException')

        # https://github.com/boto/boto3/issues/597#issuecomment-388824687


        while finished == False:
            
            retries=0
            response = {}
            while True:
                try:
                    print("Hitting GET FACE Detection")
                    response = self.rek.get_face_detection(JobId=jobId,MaxResults=maxResults,NextToken=paginationToken)
                    break
                except ClientError as err:
                    if err.response['Error']['Code'] in RETRY_EXCEPTIONS:
                        print("CATCHED ERROR ProvisionedThroughputExceededException")
                        print("---------")
                        sleep(2 ** retries)
                        retries += 1

            print("GET FACE Detection Successfull")

            for faceDetection in response['Faces']:
                print("FACES Detected")
                labelData = {}
                
                labelData["timestamp"] = faceDetection['Timestamp']

                labelData["AgeRange"] = faceDetection['Face']['AgeRange']
                labelData["Smile"] = faceDetection['Face']['Smile']
                labelData["Eyeglasses"] = faceDetection['Face']['Eyeglasses']
                labelData["Sunglasses"] = faceDetection['Face']['Sunglasses']
                labelData["Gender"] = faceDetection['Face']['Gender']
                labelData["Beard"] = faceDetection['Face']['Beard']
                labelData["Mustache"] = faceDetection['Face']['Mustache']
                labelData["EyesOpen"] = faceDetection['Face']['EyesOpen']
                labelData["MouthOpen"] = faceDetection['Face']['MouthOpen']
                labelData["Emotions"] = faceDetection['Face']['Emotions']
                labelData["Quality"] = faceDetection['Face']['Quality']
                labelData["Pose"] = faceDetection['Face']['Pose']
                
                jsonData["labels"].append(labelData)

                if 'NextToken' in response:
                    print("FACES Pagination")
                    paginationToken = response['NextToken']
                else:
                    finished = True

        return jsonData
    
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
    def testDynamoDB(self):
        dynamoDB = boto3.resource('dynamodb',
                       aws_access_key_id='',
                       aws_secret_access_key='',
                       region_name='us-east-1'
                       )
        
        table = dynamoDB.Table('bumper-ad-attributes')
        
        print(table.item_count)
        
        response = table.put_item(
                Item={
                        'VideoAd_Page': 'asdf12345',
                        'username': 'janedoe',
                        'first_name': 'Jane',
                        'last_name': 'Doe',
                        'age': 25,
                        'account_type': 'standard_user',
                }
        )
        
        print(response)




