import asyncio
from ftp import Server, MongoDBUserManager, MongoDBPathIO
from os import environ
from os.path import exists
from pyrogram import Client
from motor.motor_asyncio import AsyncIOMotorClient
if exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

async def main():
    bot = Client(
        "FTP_Bot",
        api_id=int(environ.get("API_ID", 0)),
        api_hash=environ.get("API_HASH"),
        bot_token=environ.get("BOT_TOKEN"),
        in_memory=True
    )
    await bot.start()
    loop = asyncio.get_event_loop()
    mongo = AsyncIOMotorClient(environ.get("MONGODB"), io_loop=loop).ftp
    MongoDBPathIO.db = mongo
    MongoDBPathIO.tg = bot
    MongoDBPathIO.chunk_size = 1024 * 1024 * 16 # Optional, value in bytes, this helps avoid FloodWait error, but can cause timeout error on certain ftp clients
    MongoDBPathIO.download_workers = 2 # Optional, this also can help to avoid FloodWait error, but the lower this value, the lower the download speed
    server = Server(MongoDBUserManager(mongo), MongoDBPathIO)
    print("FTP server starting...")
    await server.run(environ.get("HOST", "0.0.0.0"), int(environ.get("PORT", 9021)))

asyncio.run(main())