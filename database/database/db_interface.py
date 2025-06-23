# DATABASE
from typing import Any
import io
from database.database.engine import DBSession
from database.database.models import Base, to_dict
from sqlalchemy import exists
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import select
from typing import Any, List, Dict

DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]

class DBInterface:

    def __init__(self, db_class: type[Base]):
        self.db_class = db_class

    def read_fen(self, fen: str) -> DataObject | None:
        session: Session = DBSession()
        try:
            data: Base | None = session.get(self.db_class, fen)
            if data is None:
                return None
            return to_dict(data)
        finally:
            session.close()

    def create(self, data: DataObject) -> DataObject:
        session: Session = DBSession()
        try:
            item: Base = self.db_class(**data)
            session.add(item)
            session.commit()
            session.refresh(item)
            result = to_dict(item)
            return result
        except Exception as e:
            session.rollback()
            print(f"Error creating single item: {e}")
            raise
        finally:
            session.close()

    def create_all(self, data: ListOfDataObjects) -> bool:
        """
        Inserts multiple records using bulk_insert_mappings.
        """
        if not data:
            return 'No DATA, are you joking? it's not funny'
        session: Session = DBSession()
        try:
            session.bulk_insert_mappings(self.db_class, data)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"Error during bulk insert for {self.db_class.__tablename__}: {e}")
            raise
        finally:
            session.close()

    def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
        session: Session = DBSession()
        try:
            item: Base | None = session.get(self.db_class, primary_key_value)
            if item is None:
                return None
            for key, value in data.items():
                if hasattr(item, key):
                    setattr(item, key, value)
            session.commit()
            session.refresh(item)
            return to_dict(item)
        except Exception as e:
            session.rollback()
            print(f"Error updating item with key {primary_key_value}: {e}")
            raise
        finally:
            session.close()

    def delete(self, primary_key_value: Any) -> DataObject | None: # Made primary_key_value generic
        session: Session = DBSession()
        try:
            item: Base | None = session.get(self.db_class, primary_key_value)
            if item is None:
                return None
            result = to_dict(item)
            session.delete(item)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            print(f"Error deleting item with key {primary_key_value}: {e}")
            raise
        finally:
            session.close()

# Your to_dict function (assuming it's in models.py or accessible)
# def to_dict(obj: Base) -> dict[str, Any]:
#     return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
# DataObject = dict[str, Any]

# class DBInterface:
    
#     def __init__(self, db_class: type[Base]):
#         self.db_class = db_class

#     def read_fen(self, fen: str)->DataObject:
#         session = DBSession()
#         data: Base = session.query(self.db_class).get(fen)
#         session.close()
#         if data == None:
#             return None
#         return to_dict(data)
#     def create(self, data: DataObject) -> DataObject:
#         session = DBSession()
#         item: Base = self.db_class(**data)
#         session.add(item)
#         session.commit()
#         result = to_dict(item)
#         session.close()
#         return result   

#     def create_all(self, data: list[dict[str, Any]]) -> bool:
#         session: Session = DBSession()
#         try:
#             # Get the underlying raw psycopg2 connection
#             # This might vary slightly depending on your engine setup
#             # For a basic setup, session.connection().connection is often the psycopg2 connection
#             conn = session.connection().connection

#             # Create a in-memory file-like object
#             output = io.StringIO()
            
#             # Format data for COPY FROM: tab-separated values
#             # Assuming 'data' elements are like {'fen': 'FEN_STRING'}
#             # And 'rawfen' table has a 'fen' column
#             for row_dict in data:
#                 output.write(f"{row_dict['fen']}\n") # Write FEN and a newline
#             output.seek(0) # Go to the beginning of the stream

#             # Use psycopg2's copy_from method
#             # table_name should be 'rawfen'
#             # columns specifies the columns you are inserting into
#             with conn.cursor() as cursor:
#                 cursor.copy_from(output, self.db_class.__tablename__, columns=['fen']) # self.db_class.__tablename__ gets the table name

#             session.commit() # Commit the transaction after copy
#             return True
#         except Exception as e:
#             session.rollback()
#             print(f"Error during psycopg2 copy_from: {e}")
#             raise
#         finally:
#             session.close()

#     # def create_all(self, data: list[dict[str, Any]]) -> bool: # Changed type hint for clarity
#     #     session = DBSession()
#     #     try:
#     #         # 'data' is already a list of dictionaries, which is what bulk_insert_mappings expects
#     #         # For the RawfenCreateData(**{'fen':x}).model_dump() conversion you have,
#     #         # 'data' will be a list of {'fen': 'some_fen_string'} dictionaries.
#     #         session.bulk_insert_mappings(self.db_class, data)
#     #         session.commit()
#     #         return True
#     #     except Exception as e:
#     #         session.rollback() # Important: rollback on error
#     #         print(f"Error during bulk insert: {e}")
#     #         raise # Re-raise to propagate the error
#     #     finally:
#     #         session.close()
#     # def create_all(self, data: DataObject) -> DataObject:
#     #     session = DBSession()
#     #     item: Base = [self.db_class(**game) for game in data]
#     #     session.add_all(item)
#     #     session.commit()
#     #     session.close()
#     #     return True

#     def update(self, player_name: str, data: DataObject) -> DataObject:
#         session = DBSession()
#         item: Base = session.query(self.db_class).get(player_name)
#         for key, value in data.items():
#             setattr(item, key, value)
#         session.commit()
#         session.close()
#         return to_dict(item)
#     def delete(self, player_name: str) -> DataObject:
#         session = DBSession()
#         item: Base = session.query(self.db_class).get(player_name)
#         result = to_dict(item)
#         session.delete(item)
#         session.commit()
#         session.close()
#         return result

