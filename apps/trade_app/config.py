import requests
from microservices_connector.Interservices import Microservice, SanicApp, timeit, Friend
from configparser import ConfigParser
import click
import os
import datetime
from ..initdb import Base, engine, db_session

# enviroment = 'ENV'

# import config from file
config = ConfigParser()
config.read('config.env')

Micro = Microservice(__name__)


@Micro.app.route('/')
def helloworld():
    return 'Hello World'


def init_db():
    """Initiate all database. This should be use one time only
    """
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    from . import models
    Base.metadata.create_all(bind=engine)

# command line to start project


@click.command()
@click.option('--env', default='ENV', help='Setup enviroment variable.')
def main(env='ENV'):
    """Running method for Margin call app
    
    Keyword Arguments:
        env {str} -- Can choose between initdb, PROD, or ENV/nothing. 'initdb' will create database table and its structures, use onetime only .PROD only change debug variable in webapp to true (default: {'ENV'})
    """

    if env == 'initdb':
        init_db()
    else:
        env = str(env)
        debug = bool(config[env]['debug'] == 'True')
        Micro.run(host=config[env]['host'], port=int(
            config[env]['port']), debug=debug)
