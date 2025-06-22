import os
import requests
import pandas as pd
import tempfile
import psycopg2
from itertools import chain
from constants import CONN_STRING, PORT

def get_ask_connection():    
    return psycopg2.connect(CONN_STRING, port = PORT)
def open_request(sql_question:str):
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            sql_question
        )
        result = curs.fetchall()
    return result
def get_all_tables():
    conn = get_ask_connection()
    conn.autocommit = True
    with conn.cursor() as curs:
        curs.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
            """
        )
        tables = curs.fetchall()
    return tables
def delete_all_leela_tables():
    for table_row in ['fen','leela_scored']:
            conn = get_ask_connection()
            conn.autocommit = True
            table_name = table_row
            print(f"Deleting table: {table_name}...")
            try:
                with conn.cursor() as curs:
                    curs.execute(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE;")
                    print(f"Successfully deleted table: {table_name}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
            finally:
                if conn:
                    conn.close()
                    print("Database connection closed.")
