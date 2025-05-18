import os
from aiohttp import web
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from enum import StrEnum
import json
from time import sleep


class KAFKA_TOPICS(StrEnum):
    USER = "user-events"
    PAYMENT = "payment-events"
    MOVIE = "movie-events"


ENVS = {}
ENVS["PORT"] = int(os.environ.get("PORT", 8082))
ENVS["KAFKA_BROKERS"] = os.environ.get("KAFKA_BROKERS", "localhost:9092")


async def kafka_producer(app: web.Application):
    app["kafka_producer"] = AIOKafkaProducer(
        bootstrap_servers=ENVS["KAFKA_BROKERS"],
        client_id="event-service-producer",
    )
    await app["kafka_producer"].start()
    yield
    if app["kafka_producer"]:
        await app["kafka_producer"].stop()


async def kafka_consumer(app: web.Application):
    app["kafka_consumer"] = AIOKafkaConsumer(*(list(KAFKA_TOPICS)), bootstrap_servers=ENVS["KAFKA_BROKERS"])
    await app["kafka_consumer"].start()
    yield
    if app["kafka_consumer"]:
        await app["kafka_consumer"].stop()


async def user_event_handler(request: web.Request) -> web.Response:
    data = await request.read()
    await app["kafka_producer"].send(KAFKA_TOPICS.USER, data)
    msg = await app["kafka_consumer"].getone()
    print(msg)
    resp = {
        "status": "success",
        "partition": msg.partition,
        "offset": msg.offset,
        "event": json.loads(msg.value),
    }
    return web.json_response(data=resp)


async def payment_event_handler(request: web.Request) -> web.Response:
    data = await request.read()
    await app["kafka_producer"].send(KAFKA_TOPICS.PAYMENT, data)
    msg = await app["kafka_consumer"].getone()
    print(msg)
    resp = {
        "status": "success",
        "partition": msg.partition,
        "offset": msg.offset,
        "event": json.loads(msg.value),
    }
    return web.json_response(data=resp)


async def movie_event_handler(request: web.Request) -> web.Response:
    data = await request.read()
    await app["kafka_producer"].send(KAFKA_TOPICS.MOVIE, data)
    msg = await app["kafka_consumer"].getone()
    print(msg)
    resp = {
        "status": "success",
        "partition": msg.partition,
        "offset": msg.offset,
        "event": json.loads(msg.value),
    }
    return web.json_response(data=resp)


app = web.Application()
app.cleanup_ctx.append(kafka_producer)
app.cleanup_ctx.append(kafka_consumer)
app.add_routes([web.route("post", r"/api/events/user", user_event_handler),
                web.route("post", r"/api/events/payment", payment_event_handler),
                web.route("post", r"/api/events/movie", movie_event_handler)])


if __name__ == '__main__':
    sleep(10)
    web.run_app(app, port=ENVS["PORT"])