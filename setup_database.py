from motor.motor_asyncio import AsyncIOMotorClient
from asyncio import run
from os import environ
from os.path import exists
if exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

async def main():
    mongo = AsyncIOMotorClient(environ.get("MONGODB")).ftp
    for collection in ["users", "files"]:
        try:
            await mongo.create_collection(collection)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    run(main())