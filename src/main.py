#!env/bin/python3

from db import get_db
from settings import DB_URL, TELEGRAM_BOT_TOKEN
from querying import query_from_json_string, aggregate_salaries

from aiogram import Bot, Dispatcher, executor
from aiogram.dispatcher.filters import Command

import json

bot = Bot(TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

sampleDB = get_db(DB_URL, "sampleDB")
sample_colletions = sampleDB["sample_collection"]

@dp.message_handler(Command("start"))
async def on_start_message(message):
    await message.answer("Привет!")

@dp.message_handler()
async def on_message(message):
    try:
        input = message.text
    except ValueError | KeyError: # it's not json or not valid model
        await message.answer("Пожалуйста, дайте мне валидный json")
        return

    query = query_from_json_string(input)
    result = aggregate_salaries(query, sample_colletions)

    result["labels"] = [
        t.strftime("%Y-%m-%dT%H:%M:%S")
        for t in result["labels"]
    ] 

    await message.answer(json.dumps(result))

if __name__ == "__main__":
     executor.start_polling(dp, skip_updates=True)
