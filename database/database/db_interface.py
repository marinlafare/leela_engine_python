import os
from typing import Any, List, Dict, TypeVar
from sqlalchemy import select, insert, Integer # Import Integer
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from database.database.engine import AsyncDBSession
# Import all models that DBInterface might interact with or that are used in `create_all` logic
from database.database.models import Base, Fen, MainFen, ProcessedGame, to_dict # Ensure MainFen and ProcessedGame are also imported
from sqlalchemy.dialects.postgresql import insert as pg_insert # Import for ON CONFLICT
from datetime import datetime, timezone # For datetime.now(timezone.utc) in upsert


_ModelType = TypeVar("_ModelType", bound=Base)

DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]

class DBInterface:
    def __init__(self, db_class: TypeVar('_ModelType', bound=Base)):
        self.db_class = db_class

    async def create(self, data: DataObject) -> DataObject:
        """
        Creates a single new record.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType = self.db_class(**data)
                session.add(item)
                await session.commit()
                await session.refresh(item)
                result = to_dict(item)
                return result
            except Exception as e:
                await session.rollback()
                print(f"Error creating single item for {self.db_class.__tablename__}: {e}")
                raise

    async def read(self, **filters) -> ListOfDataObjects:
        """
        Reads records from the database based on filters.
        Returns a list of dictionaries.
        """
        async with AsyncDBSession() as session:
            try:
                stmt = select(self.db_class).filter_by(**filters)
                result = await session.execute(stmt)
                return [to_dict(row) for row in result.scalars().all()]
            except Exception as e:
                print(f"Error reading from {self.db_class.__tablename__} with filters {filters}: {e}")
                raise

    async def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
        """
        Updates an existing record identified by its primary key.
        """
        async with AsyncDBSession() as session:
            try:
                item: _ModelType | None = await session.get(self.db_class, primary_key_value)
                if item is None:
                    return None
                for key, value in data.items():
                    if hasattr(item, key): # Ensure attribute exists before setting
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
                if item is None:
                    return None
                result = to_dict(item) # Capture data before deletion
                await session.delete(item)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                print(f"Error deleting item {primary_key_value} for {self.db_class.__tablename__}: {e}")
                raise

    def get_session(self):
        """Returns an AsyncDBSession context manager."""
        return AsyncDBSession()

    async def create_all(self, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records. Handles specific UPSERT logic for Fen and MainFen.
        For other models, it uses bulk_insert_mappings.
        """
        if not data:
            print(f"No data provided for bulk insert into {self.db_class.__tablename__}.")
            return True

        async with AsyncDBSession() as session:
            try:
                if self.db_class == Fen:
                    stmt = pg_insert(self.db_class).values(data).on_conflict_do_nothing(
                        index_elements=[self.db_class.fen]
                    )
                    await session.execute(stmt)
                elif self.db_class == MainFen:
                    # UPSERT logic for MainFen: increment n_games, REPLACE moves_counter
                    stmt = pg_insert(self.db_class).values(data).on_conflict_do_update(
                        index_elements=[self.db_class.fen],
                        set_={
                            # Cast to Integer for addition, then SQLAlchemy will handle type for DB.
                            # `excluded.n_games` is already a string at this point, so cast it.
                            'n_games': (self.db_class.n_games.cast(Integer) + pg_insert(self.db_class).excluded.n_games.cast(Integer)),
                            'moves_counter': pg_insert(self.db_class).excluded.moves_counter, # Use excluded value directly (already a string)
                            'last_updated': datetime.now(timezone.utc)
                        }
                    )
                    await session.execute(stmt)
                elif self.db_class == ProcessedGame: # Assuming ProcessedGame also uses 'link' as unique ID
                    stmt = pg_insert(self.db_class).values(data).on_conflict_do_nothing(
                        index_elements=[self.db_class.link] # Assuming 'link' is the primary key/unique constraint
                    )
                    await session.execute(stmt)
                else:
                    await session.run_sync(
                        lambda sync_session: sync_session.bulk_insert_mappings(self.db_class, data)
                    )
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                print(f"Error during bulk insert/upsert for {self.db_class.__tablename__}: {e}")
                raise
