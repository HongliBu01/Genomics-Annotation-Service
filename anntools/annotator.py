import boto3
import os
import sys
import json
from subprocess import Popen
from anntools_config import Config

# QUEUE_NAME = "honglibu_job_requests"
BASE_PATH = os.path.abspath(os.path.dirname(__file__)) + '/'
PATH = BASE_PATH + "myjobs"
# TABLE = "honglibu_annotations"

def main():
  try:
    SQS = boto3.resource('sqs', region_name = Config.AWS_REGION_NAME)
    queue = SQS.get_queue_by_name(QueueName=Config.AWS_SQS_JOB_REQUEST_NAME)
  except Exception as error:
    print("Bad connection when connecting SQS: " + str(error))

  while True:
    # long polling
    print("Asking SQS for 1 message...")
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)
    # receive message and retrive info
    if len(messages) > 0:
      print("Receive 1 message")
      message = messages[0]
      body_str = json.loads(message.body)['Message']
      body = json.loads(body_str)
      job_id = body["job_id"]
      user_id = body["user_id"]
      email = body["email"]
      role = body["role"]
      bucket = body["s3_inputs_bucket"]
      filename = body["input_file_name"]
      key = body["s3_key_input_file"]
      if not os.path.exists(PATH):
        os.mkdir(PATH)

      filepath =PATH + '/' + job_id + "~" + filename
      s3 = boto3.resource('s3', region_name = Config.AWS_REGION_NAME)
      try:
        s3.Bucket(bucket).download_file(key, filepath)
      except Exception as error:
        print("Bad connection when downloading file: " + str(error))
      # run run.py
      Popen([sys.executable, BASE_PATH + "run.py", filepath, job_id, user_id, email, role])

  
      # get db item
      dynamodb = boto3.resource('dynamodb', region_name = Config.AWS_REGION_NAME)
      try:
        ann_table = dynamodb.Table(Config.AWS_DYNAMODB_ANNOTATIONS_TABLE)
      except Exception as error:
        print( "No such table: " + str(error))
      
      try:
        ann_table.update_item(
            Key={'job_id': job_id},
            ConditionExpression="job_status = :p",
            UpdateExpression="SET job_status = :r",
            ExpressionAttributeValues={':p': "PENDING", ':r': "RUNNING"})

      except Exception as error:
        print( "Error when updating table: " + str(error))

      message.delete()
      print("one message deleted")

if __name__ == '__main__':
  main()
