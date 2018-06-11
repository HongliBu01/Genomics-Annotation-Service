import boto3
import json
from util_config import Config

# QUEUE_NAME = "honglibu_job_results"
# SENDER = "honglibu@ucmpcs.org"

# ses send email when annotation finished
def send_email_ses(recipients=None,
	sender=None, subject=None, body=None):

	ses = boto3.client('ses', region_name= Config.AWS_REGION_NAME)

	response = ses.send_email(
		Destination = {'ToAddresses': recipients},
		Message={'Body': {'Text': {'Charset': "UTF-8", 'Data': body}},
		'Subject': {'Charset': "UTF-8", 'Data': subject},
		}, Source=sender)
	return response['ResponseMetadata']['HTTPStatusCode']


def main():
	try:
		SQS = boto3.resource('sqs', region_name = Config.AWS_REGION_NAME)
		queue = SQS.get_queue_by_name(QueueName=Config.AWS_SQS_JOB_COMPLETE_NAME)
	except Exception as error:
		print("Bad connection when connecting SQS: " + str(error))

	while True:
    # long polling
		print("Asking SQS for 1 message...")
		messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)

		if len(messages) > 0:
			print("Receive 1 message")
			message = messages[0]
			body_str = json.loads(message.body)['Message']
			body = json.loads(body_str)
			job_id = body["job_id"]
			user_id = body["user_id"]
			email = body["email"]

			#send a email and construct email body
			email_body = "<p>Dear %s</p><p>Your Job %s has been completed!</p>" % (user_id, job_id)
			email_subject = "Job " + job_id + " Completed!"
			try:
				send_email_ses(recipients=[email], sender=Config.MAIL_DEFAULT_SENDER, subject=email_subject, body=email_body)
			except Exception as error:
				print("Invalid parameters or Bad Conditions!" + str(error))

			print ("email sent")
			message.delete()
			print("one message deleted")

if __name__ == '__main__':
	main()

