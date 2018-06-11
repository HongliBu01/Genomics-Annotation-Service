# views.py
#
# Copyright (C) 2011-2018 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'


import uuid
import time
import json
from datetime import datetime
import time
import boto3
import stripe
from botocore.client import Config
from boto3.dynamodb.conditions import Key

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():

  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']
  profile = get_profile(identity_id=user_id)
  role = profile.role
  if role == 'free_user':
    size = 153600
  else:
    size = -1;

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + str(uuid.uuid4()) + '~${filename}'

  # Redirect to a route that will call the annotator
  redirect_url = str(request.url) + "/job"


  # Define policy conditions
  # NOTE: We also must inlcude "x-amz-security-token" since we're
  # using temporary credentials via instance roles
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  expires_in = app.config['AWS_SIGNED_REQUEST_EXPIRATION']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]


  # Generate the presigned POST call
  presigned_post = s3.generate_presigned_post(Bucket=bucket_name, 
    Key=key_name, Fields=fields, Conditions=conditions, ExpiresIn=expires_in)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post, size = size)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
  # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  key_name = request.args.get('key')
  # Extract the job ID from the S3 key
  job_id = key_name[:key_name.index('~')][-36:] #uuid4
  file_name = key_name[key_name.index('~')+1: ] # filename
  submit_time = int(time.time())

  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)
  # Persist job to database
  data = {
    "job_id": job_id,
    "user_id": user_id,
    "input_file_name": file_name,
    "s3_inputs_bucket": bucket_name,
    "s3_key_input_file": key_name,
    "submit_time": submit_time,
    "job_status": "PENDING"
  }

  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  ann_table.put_item(Item=data)

  # Send message to request queue
  message = data
  message["email"] = profile.email
  message["role"] = profile.role
  sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  sns_response = sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], Message=json.dumps(message))

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']
  #get dynamodb
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  #query 
  response = ann_table.query(
    IndexName='user_id-index',
    KeyConditionExpression=Key('user_id').eq(user_id)
  )
  # change time format to display
  for item in response['Items']:
    item['submit_time'] = datetime.fromtimestamp(item['submit_time']).strftime('%Y-%m-%d %H:%M:%S')

  return render_template('annotations.html', annotations=response)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  cur_user_id = session.get('primary_identity')
  role = get_profile(identity_id=cur_user_id).role
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  #query 
  response = ann_table.query(
    KeyConditionExpression=Key('job_id').eq(id)
  )
  items = response['Items']
  # if no such item
  if len(items) <= 0:
    return render_template('annotation_details.html', response='No Job')
  else:
    # check that the requested job ID belongs to the user that is currently authenticated
    item = items[0]
    user_id = str(item['user_id'])
    if(user_id != cur_user_id):
      return render_template('annotation_details.html', response='No Permission')
    else:
      job_status = item['job_status']
      item['submit_time'] = datetime.fromtimestamp(item['submit_time']).strftime('%Y-%m-%d %H:%M:%S')
      # check if job is completed
      if job_status == 'COMPLETED':
        now = int(time.time())
        time_diff = now - int(item['complete_time'])
        item['complete_time'] = datetime.fromtimestamp(item['complete_time']).strftime('%Y-%m-%d %H:%M:%S')
        view_url = str(request.url) + "/log"
        # check if need to be archived
        if role == 'free_user' and time_diff > 30*60:
          print(str(role) + " " + str(time_diff))
          print("free_user and time_diff larger than 30 minutes")
          return render_template('annotation_details.html', item = item, response = 'Archived', view_url = view_url) 
        else:
          print("free_user and time_diff smaller than 30 minutes or premium_user")
          result_bucket_name = item['s3_results_bucket']
          result_file_key = item['s3_key_result_file']
          # generate presigned url
          s3 = boto3.client('s3', 
            region_name=app.config['AWS_REGION_NAME'], 
            config=Config(signature_version='s3v4'))
          download_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
              'Bucket': result_bucket_name,
              'Key': result_file_key
              }
            )
          return render_template('annotation_details.html', item = item, response = 'Completed', download_url = download_url, view_url = view_url)
      else:
        return render_template('annotation_details.html', item = item, response = 'Not Completed')



"""Display the log file for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  cur_user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  #query 
  response = ann_table.query(
    KeyConditionExpression=Key('job_id').eq(id)
  )
  items = response['Items']
   # if no such item
  if len(items) <= 0:
    return render_template('log.html', response='No Job')
  else:
    # check that the requested job ID belongs to the user that is currently authenticated
    item = items[0]
    user_id = str(item['user_id'])
    if(user_id != cur_user_id):
      return render_template('log.html', response='No Permission')
    else:
      job_status = item['job_status']
      # check if job is completed
      if job_status == 'COMPLETED':
        result_bucket_name = item['s3_results_bucket']
        result_log_key = item['s3_key_log_file']
        # log text read and formatting 
        s3 = boto3.resource('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version='s3v4'))
        log_file = s3.Object(result_bucket_name, result_log_key)
        log_text = log_file.get()['Body'].read().decode('utf-8').split('\n')
        return render_template('log.html', response = 'Completed',log_text = log_text)
      else:
        return render_template('log.html', log_text = None, response = 'Not Completed')

  


"""Subscription management handler
"""

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if request.method == 'GET':
    user_id = session.get('primary_identity')
    profile = get_profile(identity_id=user_id)
    return render_template('subscribe.html')

  else:
    # set stripe and create customer
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    stripe_token = request.form.get('stripe_token')
    user_id = session.get('primary_identity')
    profile = get_profile(identity_id=user_id)
    stripe_response = stripe.Customer.create(
      description=profile.name,
      source=stripe_token,
      email=profile.email
    )

    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    db_response = table.query(
      IndexName='user_id-index',
      KeyConditionExpression=Key('user_id').eq(user_id)
    )
    items = db_response['Items']
    glacier_client = boto3.client('glacier', region_name=app.config['AWS_REGION_NAME'])
    # check every job belonging to this user
    for item in items:
      job_id = item['job_id']
      if 'results_file_archive_id' in item:
        archiveId = str(item['results_file_archive_id'])
        try:
          # initiate job in glacier and set necessary info through description
          initiate_response = glacier_client.initiate_job(
            accountId='-',
            jobParameters={
              'Tier': 'Expedited',
              'SNSTopic': app.config['AWS_SNS_JOB_RESTORE_TOPIC'],
              'Description': json.dumps({"job_id": job_id, "user_id": user_id}),  # dump into sns
              'Type': 'archive-retrieval',
              'ArchiveId': archiveId
            },
            vaultName=app.config['AWS_GLACIER_VAULT'],
          )
          retrive_job_id = initiate_response['jobId']
          print("sns message sent")
        except Exception as error:
          return redirect(url_for('internal_error'))
    stripe_id = stripe_response['id']
    update_profile(
      identity_id=user_id,
      role="premium_user")
    return render_template('subscribe_confirm.html', stripe_id = stripe_id)


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info('Login attempted from IP {0}'.format(request.remote_addr))
  # If user requested a specific page, save it to session for redirect after authentication
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. Please check the URL and try again."), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. If you think you deserve to be granted access, please contact the supreme leader of the mutating genome revolutionary party."), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; get your act together, hacker!"), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could not process your request."), 500

### EOF