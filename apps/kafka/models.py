import sys
import os
from sqlalchemy import ForeignKey
from sqlalchemy import asc, desc, func
from sqlalchemy.sql.expression import and_, or_, exists
from sqlalchemy import Column, Integer, Unicode, String, DateTime, Boolean, Numeric, Text, Date, UniqueConstraint, UnicodeText, Index, Float
from sqlalchemy.orm import relationship, backref
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy()
import datetime
import hashlib
import json
import uuid, random

class Group(db.Model):
    __tablename__ = 'Group'

    id = db.Column(db.Integer , primary_key=True)
    group_name = db.Column(db.String(500), nullable=False)
    createDate = db.Column(db.DateTime, nullable=False,default=datetime.datetime.now)
    updateDate = db.Column(db.DateTime, nullable=False,default=datetime.datetime.now)

    def __init__(self,group_name):
        self.group_name  = group_name
        self.createDate = datetime.now()
        self.updateDate = datetime.now()
    def __repr__(self):
        return '<NameGroup %r>' % self.group_name

class User(db.Model):
    __tablename__ = 'User'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(500), unique=True, nullable=False)
    password = db.Column(db.String(500), nullable=False)

    passwordresettoken = db.Column(db.String(500))
    passwordresetexpires = db.Column(db.DateTime)
    confirmed = db.Column(db.Boolean, default=False)
    confirmed_on = db.Column(db.DateTime)
    email = db.Column(db.String(500), unique=True, nullable=False)
    phone = db.Column(db.String(500), unique=True, nullable=False)
    facebook = db.Column(db.String(500))
    google = db.Column(db.String(500))
    linkin = db.Column(db.String(500))
    createdate = db.Column(db.DateTime, nullable=False,
                           default=datetime.datetime.now)
    updatedate = db.Column(db.DateTime, nullable=False,
                           default=datetime.datetime.now)
    group_id = db.Column(db.Integer, db.ForeignKey('Group.id'), nullable=False)
    group = db.relationship('Group', backref=db.backref('Group_Role', lazy=True))

    def __init__(self, username, passwordresettoken, passwordresetexpires, password, email, phone, facebook, google, linkin, group_id):
        """This function is using so many parameter. Function should only have no more than 7 parameter only
        
        Arguments:
            username {[type]} -- [description]
            passwordresettoken {[type]} -- [description]
            passwordresetexpires {[type]} -- [description]
            password {[type]} -- [description]
            email {[type]} -- [description]
            phone {[type]} -- [description]
            facebook {[type]} -- [description]
            google {[type]} -- [description]
            linkin {[type]} -- [description]
            group_id {[type]} -- [description]
        """

        self.username = username
        self.passwordresettoken = passwordresettoken
        self.passwordresetexpires = passwordresetexpires
        self.password = password
        self.email = email
        self.phone = phone
        self.facebook = facebook
        self.google = google
        self.linkin = linkin
        self.createdate = datetime.now()
        self.updatedate = datetime.now()
        self.group_id = group_id



class Account(db.Model):
    __tablename__ = 'Account'

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer)
    account_no = Column(String(30), unique=True, nullable=False)
    subkey = Column(String(255),  nullable=False, index=True)
    description = Column(String(255),  nullable=False)
    type = Column(String(10),  nullable=False, index=True)
    currency = Column(String(10),  nullable=False, index=True)
    balance = Column(Float,    nullable=False, default=0, index=True)

    live = Column(Boolean,  nullable=False, default=True)
    createAt = Column(DateTime,   nullable=False, default=datetime.datetime.now, index=True)

    def __init__(self, user_id, account_no, type='0001', description='', currency='VND', balance=0.0):
        self.user_id = user_id
        self.account_no = account_no
        self.subkey = uuid.uuid4().hex # will replace by real subkey later
        self.type = type
        self.description = description
        self.currency = currency
        self.balance = balance

   

