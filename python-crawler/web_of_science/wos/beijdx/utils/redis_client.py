#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
from utils import config

class RedisClient(object):
    def __init__(self):
        pass

    def _get_connection_pool(self):
        pool = redis.ConnectionPool(host=config.redis_config.get("host"),
                               port=config.redis_config.get("port"),
                               decode_responses = config.redis_config.get("decode_responses"))
        return pool

    def get_connection(self):
        try:
            pool = self._get_connection_pool()
            redis_client = redis.Redis(connection_pool=pool)
            return redis_client
        except redis.ConnectionError as e:
            print(e)
