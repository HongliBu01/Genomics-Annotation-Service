import json
import boto3
import time
from boto3.dynamodb.conditions import Key, Attr
from util_config import Config

# TABLE = "honglibu_annotations"
# VAULT = 'ucmpcs'
# SQS_NAME = 'honglibu_job_restore'
# BUCKET = "gas-results"

def main():
    try:
        # Get the service resource
        sqs = boto3.resource('sqs', region_name=Config.AWS_REGION_NAME)
        queue = sqs.get_queue_by_name(QueueName=Config.AWS_SQS_JOB_RESTORE_NAME)
    except Exception as error:
        print("Bad connection when connecting SQS: " + str(error))

    while True:
        # Attempt to read a message from the queue using long polling
        print("Asking SQS for 1 message...")
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)

        if len(messages) > 0:
            print("Receive 1 message")
            message = messages[0]
            body_str = json.loads(message.body)['Message']
            body = json.loads(body_str)
            body_inner = json.loads(body['JobDescription'])
            # parse parameters
            job_id = body_inner["job_id"]
            archive_id = body["ArchiveId"]
            user_id = body_inner["user_id"]
            retrive_job_id = body["JobId"]
            glacier_client = boto3.client('glacier', region_name=Config.AWS_REGION_NAME)
            restore_response = glacier_client.get_job_output(
                vaultName=Config.AWS_GLACIER_VAULT,
                jobId=retrive_job_id,
            )
            # get data from streamingbody
            s3 = boto3.resource('s3')

            dynamodb = boto3.resource('dynamodb', region_name=Config.AWS_REGION_NAME)
            table = dynamodb.Table(Config.AWS_DYNAMODB_ANNOTATIONS_TABLE)
            response = table.query(

                KeyConditionExpression=Key('job_id').eq(job_id)
            )
            items = response['Items']
            item = items[0]
            bucket = str(item['s3_results_bucket'])
            result_file_key = str(item['s3_key_result_file'])
            streaming = restore_response['body'].read()

            # put data into s3
            s3obj = s3.Object(bucket, result_file_key)
            s3obj.put(Body=streaming)
            print ("s3 file uploaded")

            # remove archive attribute from dynamodb
            table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression='REMOVE results_file_archive_id',
            )
            message.delete()

if __name__ == "__main__":
    main()