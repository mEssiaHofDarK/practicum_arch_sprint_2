import aiohttp
import os
import random
from aiohttp import web


ENVS = {}


ENVS["PORT"] = int(os.environ.get("PORT", 8000))
ENVS["MONOLITH_URL"] = os.environ.get("MONOLITH_URL", "http://monolith:8080")
ENVS["MOVIES_SERVICE_URL"] = os.environ.get("MOVIES_SERVICE_URL", "http://movies-service:8081")
ENVS["EVENTS_SERVICE_URL"] = os.environ.get("EVENTS_SERVICE_URL", "http://events-service:8082")
ENVS["MOVIES_MIGRATION_PERCENT"] = int(os.environ.get("MOVIES_MIGRATION_PERCENT", 50))/100
feat_flag = os.environ.get("GRADUAL_MIGRATION", True)
ENVS["GRADUAL_MIGRATION"] = True if feat_flag in ("true", "True", "1", True) else False


async def client_session(app: web.Application):
    app["client_session"] = aiohttp.ClientSession()
    yield
    if app["client_session"]:
        await app["client_session"].close()


@web.middleware
async def middleware(request: web.Request, handler):
    new_request = request.clone(host=ENVS["MONOLITH_URL"].replace("http://", ""))
    if request.path == "/api/movies":
        rnd_v = random.random()
        if ENVS["GRADUAL_MIGRATION"] and rnd_v < ENVS["MOVIES_MIGRATION_PERCENT"]:
            new_request = request.clone(host=ENVS["MOVIES_SERVICE_URL"].replace("http://", ""))
    elif "/api/events" in request.path:
        new_request = request.clone(host=ENVS["EVENTS_SERVICE_URL"].replace("http://", ""))
    resp = await handler(new_request)
    return resp


async def proxy_handler(request: web.Request):
    method = (request.method).lower()
    req = app["client_session"].__getattribute__(method)
    async with req(request.url) as resp:
        return web.json_response(await resp.json())


app = web.Application(middlewares=[middleware])
app.cleanup_ctx.append(client_session)
app.add_routes([web.route("*", r"/{tail:.*}", proxy_handler)])


if __name__ == '__main__':
    web.run_app(app, port=ENVS["PORT"])
