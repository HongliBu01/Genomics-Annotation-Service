import boto3
import json
import time
import uuid

def main():
	count = 0
	while True:
		print (f'Test {count}\n')
		count = count + 1
		data = {
			"load_test": True,
			"job_id": str(uuid.uuid4()),
			"email":"honglibu@uchicago.edu",
			"role": "premium_user",
			"user_id": 'autoTester',
			"input_file_name": 'test.vcf',
			"s3_inputs_bucket": "mpcs-students",
			"s3_key_input_file": "honglibu/test.vcf",
			"submit_time": int(time.time()),
			"job_status": "PENDING",
			}

		try:
			dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
			ann_table = dynamodb.Table("honglibu_annotations")
			ann_table.put_item(Item=data)
		except Exception as error:
			print (error)

		try:
			sns = boto3.client('sns', region_name="us-east-1")
			arn = "arn:aws:sns:us-east-1:127134666975:honglibu_job_requests"
			message = json.dumps(data)
			sns.publish(TopicArn=arn, Message=message)
		except Exception as error:
			print(error)

if __name__ == '__main__':
	main()
 