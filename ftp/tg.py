from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId
from pyrogram.session import Session, Auth
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.functions.auth import ImportAuthorization, ExportAuthorization
from pyrogram.raw.types import InputDocumentFileLocation
from asyncio import sleep as asleep

class File:
    def __init__(self, id, client):
        self.id = FileId.decode(id)
        self.client = client
        self.loc = InputDocumentFileLocation(id=self.id.media_id, access_hash=self.id.access_hash, file_reference=self.id.file_reference, thumb_size=self.id.thumbnail_size)

    async def getChunkAt(self, offset=0):
        session = await get_media_session(self.client, self.id)
        try:
            return (await session.send(GetFile(location=self.loc, offset=offset, limit=1024*1024))).bytes
        except TimeoutError:
            await asleep(1)
            return await self.getChunkAt(offset)

    async def stream(self, offset=0):
        try:
            while data := await self.getChunkAt(offset):
                offset += len(data)
                yield data
                if len(data) != 1024*1024:
                    break
        except:
            pass

async def get_media_session(client, file_id):
    if not (media_session := client.media_sessions.get(file_id.dc_id, None)):
        if file_id.dc_id != await client.storage.dc_id():
            media_session = Session(client, file_id.dc_id, await Auth(client, file_id.dc_id, await client.storage.test_mode()).create(), await client.storage.test_mode(), is_media=True)
            await media_session.start()

            for _ in range(6):
                exported_auth = await client.invoke(ExportAuthorization(dc_id=file_id.dc_id))
                try:
                    await media_session.invoke(ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                    break
                except AuthBytesInvalid:
                    continue
            else:
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            media_session = Session(client, file_id.dc_id, await client.storage.auth_key(), await client.storage.test_mode(), is_media=True)
            await media_session.start()
        client.media_sessions[file_id.dc_id] = media_session
    return media_session

async def stream_file(parts, bot):
    parts.sort(key=lambda x: x["part_id"])
    parts = [p["tg_file"] for p in parts]
    for part in parts:
        file = File(part, bot)
        async for chunk in file.stream():
            yield chunk