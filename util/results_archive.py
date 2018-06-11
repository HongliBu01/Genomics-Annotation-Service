import json
import boto3
import time
from boto3.dynamodb.conditions import Key, Attr
from util_config import Config


# TABLE = "honglibu_annotations"
# VAULT = 'ucmpcs'
# SQS_NAME = 'honglibu_job_archive'

def main():
  try:
  # Get the service resource
    sqs = boto3.resource('sqs', region_name=Config.AWS_REGION_NAME)
    queue = sqs.get_queue_by_name(QueueName=Config.AWS_SQS_JOB_ARCHIVE_NAME)
  except Exception as error:
    print("Bad connection when connecting SQS: " + str(error))


  while True:
  # Attempt to read a message from the queue using long polling
    print("Asking SQS for 1 message...")
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)

    # If a message was read, extract job parameters from the message body
    # If no message was read, continue polling loop
    if len(messages) > 0:
      print("Receive 1 message")
      message = messages[0]
      body_str = json.loads(message.body)['Message']
      body = json.loads(body_str)
      # parse parameters
      complete_time = body["complete_time"]
      job_id = body["job_id"]
      role = body["role"]
      # if its free user, check whether the job has been completed for 30 minutes
      if role == "free_user":
        now = int(time.time())
        diff = now - complete_time
        if diff > 30 * 60:
        # get s3 result key from dynamodb
          try:
            dynamodb = boto3.resource('dynamodb', region_name = Config.AWS_REGION_NAME)
            table = dynamodb.Table(Config.AWS_DYNAMODB_ANNOTATIONS_TABLE)
            response = table.query(
              KeyConditionExpression=Key('job_id').eq(job_id)
            )
          except Exception as error:
            print("Bad connection when connecting Dynamodb: " + str(error))
          items = response['Items']
          item = items[0]
          result_file_key = str(item['s3_key_result_file'])
          result_bucket = str(item['s3_results_bucket'])
          s3 = boto3.resource('s3', region_name = Config.AWS_REGION_NAME)
          obj = s3.Object(result_bucket, result_file_key)
          result_text = obj.get()['Body'].read()
          try:
            # upload the archive to glacier
            glacier = boto3.client('glacier', region_name = Config.AWS_REGION_NAME)
            glacier_response = glacier.upload_archive(
              vaultName=Config.AWS_GLACIER_VAULT,
              archiveDescription='achive file for ' + job_id,
              body=result_text
              )
            archive_id = glacier_response['archiveId']
          except Exception as error:
            print("Error when uploading file to Glacier: " + str(error))
          # update the dynamodb table
          table.update_item(
            Key={
              'job_id': job_id
              },
              UpdateExpression='SET results_file_archive_id = :archiveId',
              ExpressionAttributeValues={
                ':archiveId': archive_id,
                }
            )
          print ("Archived to glacier")
          # delete the result file on s3
          s3_client = boto3.client('s3', region_name = Config.AWS_REGION_NAME)
          s3_client.delete_objects(
          Bucket=result_bucket,
          Delete={
            'Objects': [
              {
              'Key': result_file_key,
              },
            ],
          })
          print ("Result file deleted from s3")
          # delete the message from sqs
          message.delete()
          print ("Message deleted!")
          # else if it's premium user
      elif role == "premium_user":
        message.delete()
        print ("Premium user, message deleted.")

if __name__ == "__main__":
  main()
