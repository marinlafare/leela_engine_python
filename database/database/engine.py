# database/database/engine.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base # Ensure Base is imported for create_all
from urllib.parse import urlparse
import asyncpg # For direct async DB operations for creation check
import asyncio # For async functions

# Assuming Base is defined in database.database.models
# You might need to adjust this import based on your exact file structure
try:
    from database.database.models import Base
except ImportError:
    # Fallback if models are in a different path or directly in this file
    Base = declarative_base()
    print("Warning: Base not found in .models, creating a new declarative_base. "
          "Ensure your models are correctly linked to this Base.")

async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

async def init_db(connection_string: str):
    """
    Initializes the asynchronous SQLAlchemy database engine and ensures
    the database and all mapped tables exist.
    """
    global async_engine

    # Parse connection string for asyncpg (used for initial DB creation check)
    parsed_url = urlparse(connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port if parsed_url.port else 5432 # Default PostgreSQL port
    db_name = parsed_url.path.lstrip('/')

    temp_conn = None
    try:
        # Connect to a default database (e.g., 'postgres') to check/create the target database
        temp_conn = await asyncpg.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database='postgres' # Connect to a default database to perform creation
        )
        
        # Check if the target database exists
        db_exists_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"
        db_exists = await temp_conn.fetchval(db_exists_query)

        if not db_exists:
            print(f"Database '{db_name}' does not exist. Creating...")
            # Use TERMINATE to ensure no active connections prevent database drop/creation during testing
            # Not strictly needed for CREATE but good for robustness if you later add DROP
            # await temp_conn.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{db_name}' AND pid <> pg_backend_pid();")
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists (concurrent creation attempt).")
    except Exception as e:
        print(f"Error during database existence check/creation: {e}")
        raise
    finally:
        if temp_conn:
            await temp_conn.close() # Ensure the temporary connection is closed

    # Create the SQLAlchemy async engine for the actual application
    # Note: asyncpg connection string uses 'postgresql+asyncpg://', not just 'postgresql://'
    # Ensure your CONN_STRING starts with 'postgresql+asyncpg://' if using asyncpg
    if not connection_string.startswith("postgresql+asyncpg://"):
        # Attempt to convert if not already asyncpg specific
        connection_string = connection_string.replace("postgresql://", "postgresql+asyncpg://", 1)

    async_engine = create_async_engine(connection_string, echo=False) # echo=True for SQL logging

    # Ensure database tables exist using the async engine
    async with async_engine.begin() as conn:
        print("Ensuring database tables exist...")
        # run_sync is used to execute synchronous metadata operations (like create_all)
        # within an async context
        await conn.run_sync(Base.metadata.create_all)
        print("Database tables checked/created.")
        
    # Configure the sessionmaker to use this async engine
    AsyncDBSession.configure(bind=async_engine)
    print("Asynchronous database initialization complete.")

