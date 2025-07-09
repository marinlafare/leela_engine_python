# CONSTANTS

import os
from dotenv import load_dotenv
load_dotenv('this_is_not_an_env.env')
# DB
USER=os.getenv("USER")
PASSWORD=os.getenv("PASSWORD")
HOST=os.getenv('HOST')
PORT=os.getenv("PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONN_STRING = os.getenv("CONN_STRING").replace('{user}',USER)
CONN_STRING = CONN_STRING.replace('{host}',HOST)
CONN_STRING = CONN_STRING.replace('{password}',PASSWORD)
CONN_STRING = CONN_STRING.replace('{port}',PORT)
CONN_STRING = CONN_STRING.replace('{database_name}',DATABASE_NAME)

#LEELA_ENGINE_CONSTANTS

LC0_PATH = os.getenv("LEELA_EXE")
lc0_directory = os.path.dirname(LC0_PATH)
LC0_WEIGHTS_FILE = os.getenv("WEIGHTS")

#GRAPHIC_CARDS_SELECTION_FOR_LEELA

BACKEND_DEFAULT = os.getenv("BACKEND_DEFAULT")
GPU_DEFAULT = os.getenv("GPU_DEFAULT")

BACKEND_ALTERNATIVE = os.getenv("BACKEND_ALTERNATIVE")
GPU_ALTERNATIVE = os.getenv("GPU_ALTERNATIVE")


