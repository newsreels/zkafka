import io
import struct
from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import avro, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka import avro, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
import os
import traceback
from datetime import datetime
from dateutil import parser
from . import bugsnagLogger as bugsnag
import uuid
import threading
import json

VERBOSE = os.getenv("KAFKA_VERBOSE")

class BaseClient:
    def __init__(self, topic, schemapath=None):
        schema_path = schemapath or os.getenv("KAFKA_SCHEMA_PATH")
        schema_str = ""
        self.schema_json = {}
        if schema_path:
            with open(schema_path) as fr:
                schema_str = fr.read()
                schema = avro.loads(schema_str)
                self.schema_json = json.loads(schema_str)
        else:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "article.json")) as fr:
                schema_str = fr.read()
                schema = avro.loads(schema_str)
                self.schema_json = json.loads(schema_str)
        if not schema:
            raise Exception("No schema provided!")
        if VERBOSE:
            print("SCHEMA_PATH: ", schema_path)
        schema_settings = {
            "url": os.getenv("KAFKA_SCHEMA_URL"),
        }
        if os.getenv("KAFKA_USE_SSL"):
            schema_settings.update({
                "ssl.ca.location": os.getenv("KAFKA_CERT_FILEPATH") or "probe",
                "ssl.key.location": os.getenv("KAFKA_SSL_PRIV_PATH"),
                "ssl.certificate.location": os.getenv("KAFKA_SSL_PUB_KEY"),
                "basic.auth.user.info": os.getenv("KAFKA_SCHEMA_API_KEY")+":"+os.getenv("KAFKA_SCHEMA_API_SECRET"), #<api-key>:<api-secret>
            })
        elif not os.getenv("KAFKA_USE_LOCAL"):
            schema_settings.update({
                "basic.auth.user.info": os.getenv("KAFKA_SCHEMA_API_KEY")+":"+os.getenv("KAFKA_SCHEMA_API_SECRET") #<api-key>:<api-secret>
            })

        settings = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
        }
        if os.getenv("KAFKA_USE_SSL"):
            settings.update({
                "security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "ssl.ca.location": os.getenv("KAFKA_CERT_FILEPATH") or "probe", #/usr/local/etc/openssl/cert.pem
                "sasl.username": os.getenv("KAFKA_API_KEY"), #<api-key>
                "sasl.password": os.getenv("KAFKA_API_SECRET"), #<api-secret>
            })
        elif not os.getenv("KAFKA_USE_LOCAL"):
            settings.update({
                "security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SASL_SSL",
                "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM") or "PLAIN",
                "sasl.username": os.getenv("KAFKA_API_KEY"), #<api-key>
                "sasl.password": os.getenv("KAFKA_API_SECRET"), #<api-secret>
            })

        self._schema_str = schema_str
        if VERBOSE:
            print("SCHEMA_STR: ", self._schema_str)
        self._client_settings = settings
        self._schema_settings = schema_settings

class Consumer(BaseClient):
    def __init__(self, topic, client_id="client-1", group_id="group-1", config={}, verbose=False, kill_event=None):
        super().__init__(topic)
        self.topic = topic
        self.verbose = verbose
        self.kill_flag = kill_event or threading.Event()
        self._schema_settings.update({
                'basic.auth.credentials.source': 'user_info'
        })
        self.register_client = CachedSchemaRegistryClient(self._schema_settings)
        settings = {
            "key.deserializer": StringDeserializer("utf-8"),
            "group.id": group_id,
            "client.id": client_id,
            "enable.auto.commit": bool(os.getenv("KAFKA_AUTOCOMMIT")),
            "session.timeout.ms": int(os.getenv("KAFKA_TIMEOUT_MS")) if os.getenv("KAFKA_TIMEOUT_MS") else 6000,
            "auto.offset.reset": "earliest"
        }
            
        if config:
            settings.update(config)

        settings.update(self._client_settings)
        if VERBOSE:
            print("_SETTINGS: ", settings)
        if VERBOSE:
            print("CONSUMER_TOPIC: ", topic)
        self.client = DeserializingConsumer(settings)
        self.client.subscribe(topic.split(",") if "," in topic else [topic])
        self.config = {
            "raise.uncommitted": bool(os.getenv("KAFKA_RAISE_UNCOMMITED")),
            "auto.commit": bool(os.getenv("KAFKA_COMMIT_PREVOUS"))
        }
        
    def stats_report(self, *args, **kwargs):
        if self.verbose:
            print("iii", args, kwargs)

    def get_kill_flag(self):
        return self.kill_flag

    def kill(self, value=True):
        if value:
            self.kill_flag.set()
        else:
            self.kill_flag.clear()

    def get_data(self, value=False):
        wait = 1
        while not self.kill_flag.is_set():
            msg = None
            try:
                if wait:
                    wait = 0
                    print("Waiting for data...")
                msg = self.client.poll(3)
            except SerializerError as e:
                traceback.print_exc()
                bugsnag.notify(e)
            except Exception as e:
                traceback.print_exc()
                bugsnag.notify(e)
                
            if msg is None:
                continue
            elif not msg.error():
                wait = 1
                if value:
                    return self.unpack(msg.value())
                else:
                    return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                wait = 1
                pass
            else:
                wait = 1
                traceback.print_exc()
                bugsnag.notify(msg.error())

    def unpack(self, payload):
        if not isinstance(payload, bytes):
            payload = payload.value()
        magic, schema_id = struct.unpack('>bi', payload[:5])

        # Get Schema registry
        # Avro value format
        if magic == 0:
            schema = self.register_client.get_by_id(schema_id)
            reader = DatumReader(schema)
            output = BinaryDecoder(io.BytesIO(payload[5:]))
            abc = reader.read(output)
            return abc
        # String key
        else:
            # If KSQL payload, exclude timestamp which is inside the key. 
            # payload[:-8].decode()
            return payload.decode()

    def commit(self, msg=None):
        self.client.commit()

    def close(self):
        self.client.close()

    def assign_partition(self, num, *args):
        topics = []
        if isinstance(num, list) or isinstance(num, tuple):
            for x in num:
                topics.append(TopicPartition(self.topic, num, *args))
        elif isinstance(num, int):
            topics.append(TopicPartition(self.topic, num, *args))
        else:
            raise Exception("Invalid argument num type:{}. Must be int or array of int".format(str(type(num))))
        self.client.assign(topics)
        

class Producer(BaseClient):
    def __init__(self, topic, config={}, verbose=False, prune=True, schemapath=None):
        super().__init__(topic, schemapath)
        self.topic = topic
        self.verbose = verbose
        self.prune = prune
        if VERBOSE:
            print("SCHEMA_SETTINGS: ", self._schema_settings)
        self.schema_registry_client = SchemaRegistryClient(self._schema_settings)
        avro_serializer = AvroSerializer(self._schema_str, self.schema_registry_client, self.serialize)
        settings = {
            "on_delivery": self.delivery_report,
            "key.serializer": StringSerializer('utf-8'),
            "value.serializer": avro_serializer,
            "stats_cb": self.stats_report,
        }

        if config:
            settings.update(config)

        settings.update(self._client_settings)
        if VERBOSE:
            print("PRODUCER_SETTINGS: ", settings)
        self.client = SerializingProducer(settings)

    def serialize(self, data, ctx):
        # keys = ["link", "source", "parentText", "category", "title", "bullets", "fullarticle", "image", "time", "keywords", "language", "parasum", "tag"]
        keys = [x['name'] for x in self.schema_json['fields']]
        if "articleimagelink" in data:
            data["image"] = data["articleimagelink"]
        if "time" in data:
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
        if self.prune:
            prune = []
            for key in data:
                if key not in keys:
                    prune.append(key)
            for key in prune:
                del data[key]
            return data

    def stats_report(self, *args, **kwargs):
        if self.verbose:
            print("iii", args, kwargs)

    def delivery_report(self, err, msg):
        if err is not None:
            print("Delivery failed: {}".format(err))
            bugsnag.notify(err)
        else:
            if self.verbose:
                print("Delivered {} [{}]".format(msg.topic(), msg.partition()))
            
    def send_data(self, msg, key=None, flush=False):
        self.client.poll(0)
        try:
            self.client.produce(topic=self.topic, value=msg, key=key if key else str(uuid.uuid4()))
        except ValueError as e:
            bugsnag.notify(e)
            traceback.print_exc()
        if flush:
            self.client.flush()

    def flush(self):
        return self.client.flush()


#@TODO
class Admin:
    def __init__(self):
        settings = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
        }
        self.client = AdminClient(settings)
        schema_settings = {
            "url": os.getenv("KAFKA_SCHEMA_URL"),
        }
        self.client_schema = SchemaRegistryClient(schema_settings)

    def new_topic(self, topic, partition, **kwargs):
        return self.client.create_topics([NewTopic(topic, partition, **kwargs)])
    
    def new_parition(self, topic, partition):
        return self.client.create_partitions([NewPartitions(topic, partition)])
        
    def get_subjects(self):
        return self.client_schema.get_subjects()

    def get_versions(self, subject):
        return self.client_schema.get_versions(subject)

    def get_schema_version(self, subject, version):
        return self.client_schema.get_version(subject, version)

    def schema_upsert(self, subject, schema_str, schema_type="AVRO"):
        schema = Schema(schema_str=schema_str, schema_type=schema_type)
        return self.client_schema.register_schema(subject_name=subject, schema=schema)

    def delete_topic_subject(self, topic, value=True, key=False):
        fa = self.delete_topic(topic)
        fsv, fsk = self.delete_subject(topic, value, key)
        return fa, fsv, fsk

    def delete_topic(self, topic):
        return self.client.delete_topic([topic])
    
    def delete_subject(self, topic, value=True, key=False):
        if value:
            fsv = self.client_schema.delete_subject(topic+"-value")
        if key:
            fsk = self.client_schema.delete_subject(topic+"-key")
        return fsv, fsk