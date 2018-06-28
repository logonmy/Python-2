#!/usr/bin/python
# -*- coding: utf-8 -*-

from kafka import KafkaProducer,KafkaConsumer
from kafka import KafkaClient
from kafka.errors import KafkaError
import json
from utils import config
"""
    kafka客户端工具类：提供kafka生产端和消费端连接，以及相关配置
"""

class KafkaClient(object):

    # kafka客户端初始化，从配置文件读取kafka连接配置
    def __init__(self):
        self.bootstrap_servers = config.kafka_bootstrap_servers

    # 获取生产端连接
    def get_producer(self):
        try:
            producer = KafkaProducer(
                acks = "all",
                bootstrap_servers = self.bootstrap_servers
            )
            return producer
        except KafkaError as e:
            print("数据发送至kafka topic失败！%s"%(e.message))
    """
        earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 
        latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 
        none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
    """
    # auto_offset_reset='latest',earliest
    # topic = "topic-1,topic-2"
    def get_consumer(self,group_id):
        try:
            consumer = KafkaConsumer(
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                bootstrap_servers = self.bootstrap_servers#,
                #max_poll_interval_ms=600000

            )
            return consumer
        except KafkaError as e:
            print("消费kafka数据topic失败！%s"%(e.message))
