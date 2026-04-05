import math
import secrets
import mimetypes
from info import BIN_CHANNEL, MAX_BTN
from utils import temp, get_size
from aiohttp import web
from web.utils.custom_dl import TGCustomYield, chunk_size, offset_fix
from web.utils.render_template import media_watch, error_tmplt, webapp_template
from database.ia_filterdb import get_search_results

routes = web.RouteTableDef()


@routes.get("/watch/{message_id}")
async def watch_handler(request):
    try:
        message_id = int(request.match_info['message_id'])
        return web.Response(text=await media_watch(message_id), content_type='text/html')
    except Exception as e:
        return web.Response(text=error_tmplt, content_type='text/html')

@routes.get("/download/{message_id}")
async def download_handler(request):
    try:
        message_id = int(request.match_info['message_id'])
        return await media_download(request, message_id)
    except:
        return web.Response(text=error_tmplt, content_type='text/html')
        

@routes.get("/", allow_head=True)
async def webapp_route_handler(request):
    return web.Response(text=webapp_template, content_type='text/html')

@routes.get("/api/search")
async def api_search_handler(request):
    query = request.query.get('q', '').strip()
    offset = int(request.query.get('offset', 0))
  
    files, next_offset, total_results = await get_search_results(query, offset=offset)
    
    formatted_files = []
    if files:
        for file in files:
            formatted_files.append({
                "id": str(file['_id']),
                "name": file.get('file_name', 'Unknown'),
                "size": get_size(file.get('file_size', 0))
            })
 
    return web.json_response({
        "files": formatted_files,
        "next_offset": next_offset if next_offset != '' else None,
        "total_results": total_results,
        "current_offset": offset,
        "max_btn": MAX_BTN,
        "bot_username": temp.U_NAME
    })


async def media_download(request, message_id: int):
    range_header = request.headers.get('Range', 0)
    media_msg = await temp.BOT.get_messages(BIN_CHANNEL, message_id)
    media = getattr(media_msg, media_msg.media.value, None)
    file_size = media.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace('bytes=', '').split('-')
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = request.http_range.stop or file_size - 1

    req_length = until_bytes - from_bytes

    new_chunk_size = await chunk_size(req_length)
    offset = await offset_fix(from_bytes, new_chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = (until_bytes % new_chunk_size) + 1
    part_count = math.ceil(req_length / new_chunk_size)
    body = TGCustomYield().yield_file(media_msg, offset, first_part_cut, last_part_cut, part_count,
                                      new_chunk_size)

    file_name = media.file_name if media.file_name \
        else f"{secrets.token_hex(2)}.jpeg"
    mime_type = media.mime_type if media.mime_type \
        else f"{mimetypes.guess_type(file_name)}"

    return_resp = web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        }
    )

    if return_resp.status == 200:
        return_resp.headers.add("Content-Length", str(file_size))

    return return_resp
