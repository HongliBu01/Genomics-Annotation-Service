# models.py
#
# Copyright (C) 2011-2018 Vas Vasiliadis
# University of Chicago
#
# Database models
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

from gas import db
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

"""Profile
"""
class Profile(db.Model):
  __tablename__ = 'profiles'

  identity_id = db.Column(UUID(as_uuid=True), primary_key=True)   # This is the Globus Auth identity UUID
  name = db.Column(db.String(256))
  email = db.Column(db.String(256))
  institution = db.Column(db.String(256))
  role = db.Column(db.String(32), default="free_user")
  created = db.Column(db.DateTime(timezone=True), server_default=func.now())
  updated = db.Column(db.DateTime(timezone=True), onupdate=func.now())

  def __repr__(self):
    return "<Profile(id='%s', name='%s')>" % (self.id, self.name)

### EOF