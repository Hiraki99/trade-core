"""Initiate Database and database model
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from configparser import ConfigParser

env = 'ENV'
config = ConfigParser()
config.read('config.env')
dbURL = str(config[env]['database'])
engine = create_engine(dbURL, convert_unicode=True)
# the line below is applied for flask app
# db_session = scoped_session(sessionmaker(bind=engine))

# Session to be used throughout app.
_Session = sessionmaker(bind=engine)


@contextmanager
def db_session():
    session = _Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# Declare database for using in Model
Base = declarative_base()

