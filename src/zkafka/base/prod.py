import io
import struct
from confluent_kafka import SerializingProducer, DeserializingConsumer
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

class Producer:
    def __init__(self, topic=None):
        self.topic = topic
        self.new()

    def delivery(self, err, msg):
        if err is not None:
            print(err)
        else:
            print("ok, ", msg)

    def new(self, override={}):
        conf= {**config(), **{
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": StringSerializer("utf_8")
        }}
        if override:
            conf.update(override)
        self.client = SerializingProducer(conf)
    
    def send(self, msg, key=None, flush=False, poll=True):
        if not self.topic:
            raise Exception("Invalid topic ", self.topic)
        if poll:
            self.client.poll(0)
        try:
            self.client.produce(topic=self.topic, value=msg, key=key if key else str(uuid.uuid4()))
        except Exception as e:
            bugsnag.notify(e)
            traceback.print_exc()
            return e
        finally:
            if flush:
                self.flush()
        return 0

    def flush(self):
        return self.client.flush()

