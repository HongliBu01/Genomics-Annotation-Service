#!/usr/bin/env python

################################################################################
#   Nov 17, 2011
#   Authors: Vlad Makarov, Chris Yoon
#   Language: Python
#   OS: UNIX/Linux, MAC OSX
#   Copyright (c) 2011, The Mount Sinai School of Medicine

#   Available under BSD  licence

#   Redistribution and use in source and binary forms, with or without modification,
#   are permitted provided that the following conditions are met:
#
#   Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
#   Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
#   IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
#   INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
#   BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
#   OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
#   EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
################################################################################

#import MySQLdb
import pymysql
import file_utils as fu
import file_utils as fu

def load_config(filename='config.txt'):
    fh = open(filename, "r")
    lines = []

    for line in fh:
        line = str(line).strip()
        if line.startswith('#') == False and len(line) > 0:
            lines.append(line)

    return lines

config=load_config()
host = config[0]
user = config[1]
passwd = config[2]
db = config[3]
port = int(config[4])

def conn2annotator():
    #conn = MySQLdb.connect (host = host, user = user, passwd = passwd, db = db, port = port)
    conn = pymysql.connect (host = host, user = user, passwd = passwd, db = db, port = port)
    return conn
