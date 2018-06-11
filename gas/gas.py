# gas.py
#
# Copyright (C) 2011-2018 Vas Vasiliadis
# University of Chicago
#
# Configure GAS runtime environment
# Setup loggers, create DB connection, import all GAS packages
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import json
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_object(os.environ['GAS_SETTINGS'])

# Configure logging
import logging
from logging.handlers import RotatingFileHandler

# Create a rotating log file handler
if not os.path.exists(app.config['GAS_LOG_FILE_PATH']):
  os.makedirs(app.config['GAS_LOG_FILE_PATH'])
log_file = app.config['GAS_LOG_FILE_PATH'] + "/" + app.config['GAS_LOG_FILE_NAME']
log_handler = RotatingFileHandler(log_file, maxBytes=500000, backupCount=9)

# Set the appropriate log level and format for log lines
if (app.config['GAS_LOG_LEVEL'] == 'INFO'):
  log_format = '%(asctime)s %(levelname)s: %(message)s '
  log_handler.setLevel(logging.INFO)
elif (app.config['GAS_LOG_LEVEL'] == 'DEBUG'):
  log_format = '%(asctime)s %(levelname)s: %(message)s ''[in %(pathname)s:%(lineno)d]'
  log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(logging.Formatter(log_format))

logger = logging.getLogger(app.config['WSGI_SERVER'])  # Create the WSGI server (werkzeug, gunicorn, etc.) logger
logger.addHandler(log_handler)  # Add the file log handler to the server logger
app.logger.addHandler(log_handler)  # Tell the Flask app's logger to use the file handler also
app.logger.handlers = logger.handlers  # Tell the Flask app logger to write to the WSGI server logger
app.logger.setLevel(logger.level)  # Set the app log level to the same as the WSGI server's log level

db = SQLAlchemy(app)  # Adds database handle to the Flask app

import views
import auth

### EOF