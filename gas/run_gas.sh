#!/bin/bash

# run_gas.sh
#
# Copyright (C) 2011-2018 Vas Vasiliadis
# University of Chicago
#
# Rns the GAS app using the Gunicorn server for production environments
#
##

cd /home/ubuntu/gas
source /home/ubuntu/gas/.env
[[ -d /home/ubuntu/gas/log ]] || mkdir /home/ubuntu/gas/log
if [ ! -e /home/ubuntu/gas/log/$GAS_LOG_FILE_NAME ]; then
    touch /home/ubuntu/gas/log/$GAS_LOG_FILE_NAME;
fi
if [ "$1" = "console" ]; then
    LOG_TARGET=-
else
    LOG_TARGET=/home/ubuntu/gas/log/$GAS_LOG_FILE_NAME
fi
/home/ubuntu/.virtualenvs/mpcs/bin/gunicorn --log-file=$LOG_TARGET \
--log-level=debug --workers=$GUNICORN_WORKERS --certfile=/usr/local/src/ssl/ucmpcs.org.crt \
--keyfile=/usr/local/src/ssl/ucmpcs.org.key --bind=$GAS_APP_HOST:$GAS_HOST_PORT gas:app
