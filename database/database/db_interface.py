import os
from typing import Any, List, Dict, TypeVar
from sqlalchemy import select, insert, Integer, func # Import func
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from database.database.engine import AsyncDBSession
from database.database.models import Base, Fen, to_dict
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timezone
from database.operations.models import FenGameAssociateData


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
                    if hasattr(item, key):
                        setattr(item, key, value)
                await session.commit()
                await session.refresh(item)
                return to_dict(item)
            except Exception as e:
                await session.rollback()
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
                result = to_dict(item)
                await session.delete(item)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                raise

    def get_session(self):
        """Returns an AsyncDBSession context manager."""
        return AsyncDBSession()

    async def create_all(self, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records. Handles specific UPSERT logic for Fen and MainFen.
        For other models, it uses bulk_insert_mappings.
        **This method will now chunk inserts to avoid parameter limits.**
        """
        if not data:
            return True

        # Determine the number of parameters per row for chunking
        params_per_row = 0
        if self.db_class == FromGame:
            params_per_row = 1 # 'fen'
        elif self.db_class == Fen:
            params_per_row = 5 # 'fen', 'n_games', 'moves_counter', 'score', 'next_moves'
        else: # For generic bulk_insert_mappings, assume average number of columns or use a default safe limit
            params_per_row = len(self.db_class.__table__.columns) if hasattr(self.db_class, '__table__') else 3

        INSERT_BATCH_SIZE = 5000
        # Calculate effective batch size
        if params_per_row > 0:
            effective_batch_size = min(INSERT_BATCH_SIZE, 32000 // params_per_row) # Stay well below the 32767 limit
            if effective_batch_size == 0:
                effective_batch_size = 1
        else:
            effective_batch_size = INSERT_BATCH_SIZE

        # Chunk the data
        chunks = [data[i:i + effective_batch_size] for i in range(0, len(data), effective_batch_size)]

        async with AsyncDBSession() as session:
            try:
                for i, chunk in enumerate(chunks):
                    if not chunk:
                        continue

                    if self.db_class == FromGame:
                        stmt = pg_insert(self.db_class).values(chunk).on_conflict_do_nothing(
                            index_elements=[self.db_class.link]
                        )
                    elif self.db_class == Fen:
                        stmt = pg_insert(self.db_class).values(chunk).on_conflict_do_update(
                            index_elements=[self.db_class.fen],
                            set_={
                                'n_games': (self.db_class.n_games.cast(Integer) + pg_insert(self.db_class).excluded.n_games.cast(Integer)),
                                'moves_counter': pg_insert(self.db_class).excluded.moves_counter,
                                'next_moves': None,
                                'score': None
                                
                            }
                        )

                    else:
                        # For generic bulk_insert_mappings, it's generally better to pass the full list
                        # and let SQLAlchemy handle its internal chunking for efficiency,
                        # but if it was failing, this explicit chunking would be the workaround.
                        # For now, let's keep the explicit chunking for consistency with pg_insert.
                        await session.run_sync(
                            lambda sync_session, c=chunk: sync_session.bulk_insert_mappings(self.db_class, c)
                        )
                        # We need to commit after each bulk_insert_mappings chunk if using run_sync like this
                        # because bulk_insert_mappings doesn't participate in the session's transaction
                        # in the same way as session.execute with pg_insert.
                        await session.commit()
                        continue # Skip the session.execute for bulk_insert_mappings

                    await session.execute(stmt)

                # Final commit for all pg_insert chunks within this method
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                raise

    async def upsert_main_fens(self,
                               objects_to_insert: ListOfDataObjects,
                               objects_to_update: ListOfDataObjects) -> bool:
        """
        Inserts new MainFen records and updates existing ones based on pre-separated lists.
        - Increments n_games for updates.
        - Appends moves_counter for updates ONLY IF the new moves_counter string is not already a substring
          of the existing moves_counter in the database.
        

        Args:
            objects_to_insert: List of dictionaries for new MainFen records.
            objects_to_update: List of dictionaries for existing MainFen records that need updates.
                               Each dict in this list should contain 'fen', 'n_games', 'moves_counter',
                               and 'existing_moves_counter' (the moves_counter from the DB).
        """
        if not objects_to_insert and not objects_to_update:
            return True

        async with AsyncDBSession() as session:
            try:
                # --- Process Inserts ---
                if objects_to_insert:
                    insert_stmt = pg_insert(Fen).values(objects_to_insert).on_conflict_do_nothing(
                        index_elements=[Fen.fen]
                    )
                    await session.execute(insert_stmt)

                # --- Process Updates ---
                if objects_to_update:
                    for item_data in objects_to_update:
                        fen_to_update = item_data['fen']
                        new_moves_counter = item_data['moves_counter']
                        db_item = await session.get(Fen, fen_to_update)
                        existing_moves_counter = db_item.moves_counter
                        if new_moves_counter not in existing_moves_counter:
                            updated_moves_counter = existing_moves_counter + new_moves_counter
                        else:
                            updated_moves_counter = existing_moves_counter                        
                        db_item.n_games += item_data['n_games']
                        db_item.moves_counter = updated_moves_counter
                        db_item.next_moves = item_data['next_moves']
                        db_item.score = item_data['score']

                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                raise
    async def associate_fen_with_games(self, data: FenGameAssociateData) -> bool:
        """
        Associates a given FEN with a list of games.
        This method will:
        1. Retrieve the Fen object based on data.fen.
        2. Retrieve the Game objects based on data.game_links.
        3. Establish the many-to-many relationship by appending the Fen object
           to the 'fens' collection of each Game object.
        4. Commit the changes to the database.

        Args:
            data: A FenGameAssociateData Pydantic model containing the FEN string
                  and a list of game links to associate with.

        Returns:
            True if the association was successful, False otherwise.
        """
        async with AsyncDBSession() as session:
            try:
                # 1. Retrieve the Fen object
                fen_result = await session.execute(select(Fen).filter_by(fen=data.fen))
                fen_obj = fen_result.scalars().first()

                if not fen_obj:
                    print(f"Error: FEN '{data.fen}' not found. Cannot perform association.")
                    return False

                # 2. Retrieve the Game objects
                game_results = await session.execute(select(Game).filter(Game.link.in_(data.game_links)))
                game_objs = game_results.scalars().all()

                if not game_objs:
                    print(f"No games found for links: {data.game_links}. No association made.")
                    return False

                # 3. Establish the association
                associated_count = 0
                for game in game_objs:
                    # Check if the association already exists to prevent duplicates
                    # This relies on the 'fens' relationship being loaded or lazy-loaded
                    if fen_obj not in game.fens:
                        game.fens.append(fen_obj)
                        associated_count += 1
                        print(f"Associated FEN '{fen_obj.fen}' with Game Link: {game.link}")
                    else:
                        print(f"FEN '{fen_obj.fen}' already associated with Game Link: {game.link}. Skipping.")

                if associated_count > 0:
                    await session.commit()
                    print(f"Successfully committed {associated_count} new associations for FEN '{fen_obj.fen}'.")
                    return True
                else:
                    print(f"No new associations were made for FEN '{fen_obj.fen}'.")
                    return False

            except Exception as e:
                await session.rollback()
                print(f"An error occurred during FEN-Game association: {e}")
                raise # Re-raise the exception after rollback for higher-level handling
# import os
# from typing import Any, List, Dict, TypeVar
# from sqlalchemy import select, insert, Integer # Import Integer
# from sqlalchemy.orm import Session
# from sqlalchemy.ext.asyncio import AsyncSession
# from database.database.engine import AsyncDBSession
# # Import all models that DBInterface might interact with or that are used in `create_all` logic
# from database.database.models import Base, Fen, MainFen, ProcessedGame, to_dict # Ensure MainFen and ProcessedGame are also imported
# from sqlalchemy.dialects.postgresql import insert as pg_insert # Import for ON CONFLICT
# from datetime import datetime, timezone # For datetime.now(timezone.utc) in upsert


# _ModelType = TypeVar("_ModelType", bound=Base)

# DataObject = Dict[str, Any]
# ListOfDataObjects = List[DataObject]

# class DBInterface:
#     def __init__(self, db_class: TypeVar('_ModelType', bound=Base)):
#         self.db_class = db_class

#     async def create(self, data: DataObject) -> DataObject:
#         """
#         Creates a single new record.
#         """
#         async with AsyncDBSession() as session:
#             try:
#                 item: _ModelType = self.db_class(**data)
#                 session.add(item)
#                 await session.commit()
#                 await session.refresh(item)
#                 result = to_dict(item)
#                 return result
#             except Exception as e:
#                 await session.rollback()
#                 print(f"Error creating single item for {self.db_class.__tablename__}: {e}")
#                 raise

#     async def read(self, **filters) -> ListOfDataObjects:
#         """
#         Reads records from the database based on filters.
#         Returns a list of dictionaries.
#         """
#         async with AsyncDBSession() as session:
#             try:
#                 stmt = select(self.db_class).filter_by(**filters)
#                 result = await session.execute(stmt)
#                 return [to_dict(row) for row in result.scalars().all()]
#             except Exception as e:
#                 print(f"Error reading from {self.db_class.__tablename__} with filters {filters}: {e}")
#                 raise

#     async def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
#         """
#         Updates an existing record identified by its primary key.
#         """
#         async with AsyncDBSession() as session:
#             try:
#                 item: _ModelType | None = await session.get(self.db_class, primary_key_value)
#                 if item is None:
#                     return None
#                 for key, value in data.items():
#                     if hasattr(item, key): # Ensure attribute exists before setting
#                         setattr(item, key, value)
#                 await session.commit()
#                 await session.refresh(item)
#                 return to_dict(item)
#             except Exception as e:
#                 await session.rollback()
#                 print(f"Error updating item {primary_key_value} for {self.db_class.__tablename__}: {e}")
#                 raise

#     async def delete(self, primary_key_value: Any) -> DataObject | None:
#         """
#         Deletes a record identified by its primary key.
#         """
#         async with AsyncDBSession() as session:
#             try:
#                 item: _ModelType | None = await session.get(self.db_class, primary_key_value)
#                 if item is None:
#                     return None
#                 result = to_dict(item) # Capture data before deletion
#                 await session.delete(item)
#                 await session.commit()
#                 return result
#             except Exception as e:
#                 await session.rollback()
#                 print(f"Error deleting item {primary_key_value} for {self.db_class.__tablename__}: {e}")
#                 raise

#     def get_session(self):
#         """Returns an AsyncDBSession context manager."""
#         return AsyncDBSession()

#     async def create_all(self, data: ListOfDataObjects) -> bool:
#         """
#         Inserts multiple records. Handles specific UPSERT logic for Fen and MainFen.
#         For other models, it uses bulk_insert_mappings.
#         """
#         if not data:
#             print(f"No data provided for bulk insert into {self.db_class.__tablename__}.")
#             return True

#         async with AsyncDBSession() as session:
#             try:
#                 if self.db_class == Fen:
#                     stmt = pg_insert(self.db_class).values(data).on_conflict_do_nothing(
#                         index_elements=[self.db_class.fen]
#                     )
#                     await session.execute(stmt)
#                 elif self.db_class == MainFen:
#                     # UPSERT logic for MainFen: increment n_games, REPLACE moves_counter
#                     stmt = pg_insert(self.db_class).values(data).on_conflict_do_update(
#                         index_elements=[self.db_class.fen],
#                         set_={
#                             # Cast to Integer for addition, then SQLAlchemy will handle type for DB.
#                             # `excluded.n_games` is already a string at this point, so cast it.
#                             'n_games': (self.db_class.n_games.cast(Integer) + pg_insert(self.db_class).excluded.n_games.cast(Integer)),
#                             'moves_counter': pg_insert(self.db_class).excluded.moves_counter, # Use excluded value directly (already a string)
#                             'last_updated': datetime.now(timezone.utc)
#                         }
#                     )
#                     await session.execute(stmt)
#                 elif self.db_class == ProcessedGame: # Assuming ProcessedGame also uses 'link' as unique ID
#                     stmt = pg_insert(self.db_class).values(data).on_conflict_do_nothing(
#                         index_elements=[self.db_class.link] # Assuming 'link' is the primary key/unique constraint
#                     )
#                     await session.execute(stmt)
#                 else:
#                     await session.run_sync(
#                         lambda sync_session: sync_session.bulk_insert_mappings(self.db_class, data)
#                     )
#                 await session.commit()
#                 return True
#             except Exception as e:
#                 await session.rollback()
#                 print(f"Error during bulk insert/upsert for {self.db_class.__tablename__}: {e}")
#                 raise
