import configparser

config = configparser.RawConfigParser()
config.readfp(open('util_config.cfg'))


class Config(object):
    AWS_REGION_NAME = config.get('uti.aws', 'region')
    AWS_S3_INPUTS_BUCKET = config.get('uti.aws', 'inputs_bucket')
    AWS_S3_RESULTS_BUCKET = config.get('uti.aws', 'results_bucket')

    AWS_GLACIER_VAULT = config.get('uti.aws', 'vault')

    # Change the ARNs below to reflect your SNS topics
    AWS_SNS_JOB_REQUEST_TOPIC = config.get('uti.aws', 'requests_topic')
    AWS_SNS_JOB_COMPLETE_TOPIC = config.get('uti.aws', 'results_topic')
    AWS_SNS_JOB_RESTORE_TOPIC = config.get('uti.aws', 'restore_topic')

    AWS_SQS_JOB_REQUEST_NAME = config.get('uti.aws', 'requests_sqs')
    AWS_SQS_JOB_RESTORE_NAME = config.get('uti.aws', 'restore_sqs')
    AWS_SQS_JOB_COMPLETE_NAME = config.get('uti.aws', 'results_sqs')
    AWS_SQS_JOB_ARCHIVE_NAME = config.get('uti.aws', 'archive_sqs')

    # Change the table name to your own
    AWS_DYNAMODB_ANNOTATIONS_TABLE = config.get('uti.aws', 'annotations_table')

    # Stripe API keys
    STRIPE_PUBLIC_KEY = config.get('uti.stripe', 'stripe_public_key')
    STRIPE_SECRET_KEY = config.get('uti.stripe', 'stripe_secret_key')


    MAIL_DEFAULT_SENDER = config.get('uti.email', 'sender')