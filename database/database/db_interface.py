# database/database/db_interface.py

from typing import Any, List, Dict, TypeVar
from sqlalchemy import select # For async ORM queries
from sqlalchemy.orm import Session # For type hinting synchronous session (if any)
from sqlalchemy.ext.asyncio import AsyncSession # For type hinting async session
from database.database.engine import AsyncDBSession # Import the new AsyncDBSession
from database.database.models import Base, to_dict # Assuming Base and to_dict are here

# Define a TypeVar for the Base model to improve type hinting in DBInterface
_ModelType = TypeVar("_ModelType", bound=Base)

DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]

class DBInterface:
    """
    Asynchronous database interface for CRUD operations on SQLAlchemy models.
    Uses AsyncDBSession for session management.
    """

    def __init__(self, db_class: TypeVar("_ModelType")): # Use TypeVar for db_class
        self.db_class = db_class

    async def read_fen(self, fen: str) -> DataObject | None:
        """
        Reads a single record by its primary key (FEN string).
        """
        async with AsyncDBSession() as session: # Use async with for session management
            try:
                # Use session.get for primary key lookup directly
                data: _ModelType | None = await session.get(self.db_class, fen)
                if data is None:
                    return None
                return to_dict(data)
            except Exception as e:
                print(f"Error reading single item {fen} from {self.db_class.__tablename__}: {e}")
                raise # Re-raise to propagate the error

    async def create(self, data: DataObject) -> DataObject:
        """
        Creates a single new record.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType = self.db_class(**data)
                session.add(item)
                await session.commit() # Await commit
                await session.refresh(item) # Await refresh
                result = to_dict(item)
                return result
            except Exception as e:
                await session.rollback() # Await rollback
                print(f"Error creating single item for {self.db_class.__tablename__}: {e}")
                raise

    async def create_all(self, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records using bulk_insert_mappings asynchronously.
        """
        if not data:
            print(f"No data provided for bulk insert into {self.db_class.__tablename__}.")
            return True # Or False, depending on desired behavior for empty input
            
        async with AsyncDBSession() as session:
            try:
                # bulk_insert_mappings is a synchronous method, so we run it in a thread pool
                # using session.run_sync. The lambda must accept the synchronous session object.
                await session.run_sync(
                    lambda sync_session: sync_session.bulk_insert_mappings(self.db_class, data) # CORRECTED LAMBDA
                )
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                print(f"Error during bulk insert for {self.db_class.__tablename__}: {e}")
                raise

    async def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
        """
        Updates an existing record identified by its primary key.
        """
        async with AsyncDBSession() as session:
            try:
                # Fetch the item using async session.get
                item: _ModelType | None = await session.get(self.db_class, primary_key_value)
                if item == None: # Corrected from `is None` for explicit check
                    return None
                for key, value in data.items():
                    if hasattr(item, key):
                        setattr(item, key, value)
                await session.commit()
                await session.refresh(item)
                return to_dict(item)
            except Exception as e:
                await session.rollback()
                print(f"Error updating item {primary_key_value} for {self.db_class.__tablename__}: {e}")
                raise

    async def delete(self, primary_key_value: Any) -> DataObject | None:
        """
        Deletes a record identified by its primary key.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType | None = await session.get(self.db_class, primary_key_value)
                if item == None: # Corrected from `is None` for explicit check
                    return None
                result = to_dict(item) # Capture data before deletion
                await session.delete(item) # Await delete
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                print(f"Error deleting item {primary_key_value} for {self.db_class.__tablename__}: {e}")
                raise
