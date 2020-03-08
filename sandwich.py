import asyncio
import multiprocessing

import aiohttp
import msgpack

import aiokafka

try:
    import ujson as json
except ImportError:
    import json

loop = asyncio.get_event_loop()


async def consume():
    consumer = aiokafka.AIOKafkaConsumer(
        "welcomerMain",
        loop=loop, bootstrap_servers="localhost:9092"
    )
    await consumer.start()
    print("Started consuming")
    try:
        # Consume messages
        async for msg in consumer:
            data = msgpack.unpackb(msg.value)
            print(json.dumps(data))
            # print("consumed: ", msg.topic, msg.partition, msg.offset,
            #       msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

loop.run_until_complete(consume())
