import os

DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = os.environ["DB_PORT"]

DB_URL = "mongodb://{}:{}@127.0.0.1:{}".format(
    DB_USERNAME, DB_PASSWORD, DB_PORT
)

TELEGRAM_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
