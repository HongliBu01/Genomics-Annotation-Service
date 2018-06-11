import os
import sys
import time
import boto3
import driver
import json
from anntools_config import  Config

#
# PREFIX = "honglibu/"
# BUCKET = "gas-results"
# TABLE = "honglibu_annotations"
# TOPIC_ARN_RESULT = "arn:aws:sns:us-east-1:127134666975:honglibu_job_results"



class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print("Total runtime: {0:.6f} seconds".format(self.secs))


if __name__ == '__main__':
    if(len(sys.argv) > 5):
        filename = sys.argv[1]
        job_id = sys.argv[2]
        user_id = sys.argv[3]
        email = sys.argv[4]
        role = sys.argv[5]
        with Timer():
            driver.run(filename, 'vcf')
        results_filename = filename.replace('.vcf', '.annot.vcf')
        log_filename = filename + '.count.log'

        s3 = boto3.resource('s3', region_name = Config.AWS_REGION_NAME)
        results_key = Config.AWS_S3_KEY_PREFIX + user_id + '/' + results_filename[results_filename.rfind('/')+1:]
        try:
            s3.Bucket(Config.AWS_S3_RESULTS_BUCKET).upload_file(results_filename, results_key)
            print("results uploaded!")
        except Exception as error:
            print("Bad connection when uploading results: " + str(error))


        log_key = Config.AWS_S3_KEY_PREFIX + user_id + '/' + log_filename[log_filename.rfind('/')+1:]
        try:
            s3.Bucket(Config.AWS_S3_RESULTS_BUCKET).upload_file(log_filename, log_key)
            print("logs uploaded!")
        except Exception as error:
            print("Bad connection when uploading logs: " + str(error))

        os.remove(filename)
        os.remove(results_filename)
        os.remove(log_filename)

        complete_time = int(time.time())
        dynamodb = boto3.resource('dynamodb', region_name = Config.AWS_REGION_NAME)
        try:
            ann_table = dynamodb.Table(Config.AWS_DYNAMODB_ANNOTATIONS_TABLE)
            ann_table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression='SET s3_results_bucket = :results_bucket, s3_key_result_file = :key_result_file, s3_key_log_file = :key_log_file, complete_time = :complete_time, job_status = :job_status',
                ExpressionAttributeValues={
                    ':results_bucket': Config.AWS_S3_RESULTS_BUCKET,
                    ':key_result_file': results_key,
                    ':key_log_file': log_key,
                    ':complete_time': complete_time,
                    ':job_status': 'COMPLETED'
                }
            )
            print ("database item updated!")
        except Exception as error:
            print("Bad connection when updating table: " + str(error))
        try:
            sns = boto3.client('sns', region_name= Config.AWS_REGION_NAME)
            message = json.dumps({"job_id": job_id, "complete_time": complete_time, "user_id": user_id, "email": email, "role": role})
            sns_response = sns.publish(TopicArn=Config.AWS_SNS_JOB_COMPLETE_TOPIC, Message=message)
            print ("sns message sent")
        except Exception as error:
            print("Bad connection when sending sns: " + str(error))


    else:
        print("A valid .vcf and job_id must be provided as input to this program.")
