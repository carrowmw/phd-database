# phd_project/database/database.py

# pylint: disable=import-outside-toplevel
# pylint: disable=unused-import

"""
This file handles database connections and session management using SQLAlchemy ORM.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# from sqlalchemy.dialects import postgresql
# from utils.config_helper import get_database_url

# Create a database engine
# database_url = get_database_url()
database_url = "postgresql://test@localhost:5432/testdb"
engine = create_engine(database_url)

LocalSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    from database.src.models import Base

    Base.metadata.create_all(bind=engine)


def get_db():
    db = LocalSession()
    try:
        yield db
    finally:
        db.close()
