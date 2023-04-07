from pymongo import MongoClient 

def get_db(url: str, database_name: str):
    client = MongoClient(url)
    database = client[database_name]

    return database

