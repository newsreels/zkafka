from .base import Consumer as BaseConsumer
from .base import Producer as BaseProducer
from .avro import Consumer as AvroConsumer
from .avro import Producer as AvroProducer
import os

class Consumer:
    def __init__(self, topic, client_id="client-1", group_id="group-1", config={}, verbose=False, kill_event=None, mode=""):
        if mode!="base" and (os.getenv("KAFKA_SCHEMA_URL") and os.getenv("KAFKA_SCHEMA_API_KEY") and os.getenv("KAFKA_SCHEMA_API_SECRET")):
            if verbose:
                print("AVRO CONSUMER")
            self.client = AvroConsumer(topic, client_id, group_id, config, verbose, kill_event)
        else:
            if verbose:
                print("BASE CONSUMER")
            self.client = BaseConsumer(topic, client_id, group_id, config, verbose, kill_event)
        
        self.kill_flag = self.client.kill_flag

    def consume(self, value=False, poll=3.0):
        return self.client.consume(value, poll)
    
    def get_data(self, value=False, poll=3.0):
        wait = 1
        while 1:
            if wait:
                print("waiting for data...")
                wait = 0
            msg = self.client.consume(value, poll)
            if not msg:
                continue
            return msg

    def commit(self, msg=None):
        self.client.commit(msg)

    def unpack(self, msg):
        return self.client.unpack(msg)
    
    def close(self):
        self.client.close()

class Producer:
    def __init__(self, topic=None, config={}, verbose=False, prune=True, schemapath=None, mode=""):
        if mode!="base" and os.getenv("KAFKA_SCHEMA_URL") and os.getenv("KAFKA_SCHEMA_API_KEY") and os.getenv("KAFKA_SCHEMA_API_SECRET"):
            if verbose:
                print("AVRO PRODUCER")
            self.client = AvroProducer(topic, config, verbose, prune, schemapath)
        else:
            if verbose:
                print("BASE PRODUCER")
            self.client = BaseProducer(topic, config, verbose, prune, schemapath)

    def send_data(self, msg, key=None, flush=False, poll=True):
        return self.client.send_data(msg, key, flush, poll)
    
    def flush(self):
        return self.client.flush()


class Admin:
    def __init__(self, *args, **kwargs):
        pass