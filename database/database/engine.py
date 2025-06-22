from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from .models import Base

engine: Engine = None
DBSession = sessionmaker()

def init_db(connection_string: str):
    url = connection_string
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url)
    Base.metadata.create_all(bind=engine)
    DBSession.configure(bind=engine)