import io
import struct
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.cimpl import Message
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaError
from confluent_kafka.serialization import StringDeserializer, StringSerializer
import os
import traceback
from datetime import datetime
from dateutil import parser
from .. import bugsnagLogger as bugsnag
from .config import config
import uuid
import threading
import json

class Consumer:
    def __init__(self, topic=None, client_id=None, group_id="group1", override={}):
        self.topic = topic
        self.kill_flag = False
        self.new(client_id, group_id, override)

    def kill(self, val=True):
        self.kill_flag = val

    def new(self, client_id=None, group_id=None, override={}):
        conf= {**config(), **{
            "key.deserializer": StringDeserializer("utf_8"),
            "enable.auto.commit": bool(os.getenv("KAFKA_AUTOCOMMIT")),
            "session.timeout.ms": int(os.getenv("KAFKA_TIMEOUT_MS")) if os.getenv("KAFKA_TIMEOUT_MS") else 6000,
            "auto.offset.reset": "earliest"
        }}
        if client_id:
            conf.update({
                "client.id": client_id
            })
        if group_id:
            conf.update({
                "group.id": group_id
            })
        if override:
            conf.update(override)
        self.client = DeserializingConsumer(conf)
        self.subscribe()

    def subscribe(self, topics=""):
        self.client.subscribe((self.topic + topics).split(","))

    def get_data(self, *args):
        while not self.kill_flag:
            msg = None
            try:
                print("getting message...")
                msg = self.client.poll(3.0)
            except Exception as e:
                bugsnag.notify(e)
                traceback.print_exc()
            
            if msg is None:
                print("no message")
                continue
            elif not msg.error():
                return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                traceback.print_exc()
                bugsnag.notify(msg.error())

    def unpack(self, val):
        if isinstance(val, Message):
            return val.value()
        return val
    
    def commit(self, msg: Message = None):
        if msg:
            self.client.commit(message=msg)
        else:
            self.client.commit()

    def close(self):
        self.client.close()