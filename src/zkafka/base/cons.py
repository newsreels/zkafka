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
from .config import config as client_config
import uuid
import threading
import json

class Consumer:
    def __init__(self, topic, client_id=None, group_id="group1", config={}, verbose=False, kill_event=None):
        self.topic = topic
        self.kill_flag = kill_event or threading.Event()
        self.new(client_id, group_id, config)
        self.debug = verbose

    def kill(self, val=True):
        if val:
            self.kill_flag.set()
        else:
            self.kill_flag.clear()

    def new(self, client_id=None, group_id=None, config={}):
        conf= {**client_config(), **{
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
        if config:
            conf.update(config)
        self.client = DeserializingConsumer(conf)
        self.subscribe()

    def subscribe(self, topics=""):
        self.client.subscribe((self.topic + topics).split(","))

    def get_data(self, value=False):
        while not self.kill_flag.is_set():
            msg = None
            try:
                if self.debug:
                    print("getting message...")
                msg = self.client.poll(3.0)
            except Exception as e:
                bugsnag.notify(e)
                traceback.print_exc()
            
            if msg is None:
                if self.debug:
                    print("no message")
                continue
            elif not msg.error():
                if value:
                    return msg.value()
                else:
                    return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                traceback.print_exc()
                bugsnag.notify(msg.error())

    def consume(self, value=False, poll=3.0):
        try:
            if self.debug:
                print("getting message...")
            msg = self.client.poll(poll)
            if msg is None:
                if self.debug:
                    print("no message")
                return
            elif not msg.error():
                return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                traceback.print_exc()
                bugsnag.notify(msg.error())
        except Exception as e:
            bugsnag.notify(e)
            traceback.print_exc()
    
    def poll(self, t):
        return self.client.poll(t)

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