import os
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

def get_RPC_URL():
    return os.getenv('RPC_URL')

def get_START_BLOCK():
    return int(os.getenv('START_BLOCK'))

def get_END_BLOCK():
    return int(os.getenv('END_BLOCK'))

def get_BLOCK_BATCH_SIZE():
    return int(os.getenv('BLOCK_BATCH_SIZE'))

def get_CHUNK_SIZE():
    return int(os.getenv('CHUNK_SIZE'))