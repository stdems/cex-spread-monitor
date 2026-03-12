import os
import redis
import redis.asyncio as aioredis

_host = os.getenv("REDIS_HOST", "localhost")
_port = int(os.getenv("REDIS_PORT", "6379"))

r = aioredis.Redis(host=_host, port=_port, decode_responses=True)
r_sync = redis.Redis(host=_host, port=_port, decode_responses=True)


async def update_table(table, pair, exchange, value):
    if value is None:
        return
    key = f"{table}:{pair}"
    await r.hset(key, exchange, value)


def update_table_sync(table, pair, exchange, value):
    if value is None:
        return
    key = f"{table}:{pair}"
    r_sync.hset(key, exchange, value)


async def get_table(table, pair):
    key = f"{table}:{pair}"
    return await r.hgetall(key)
