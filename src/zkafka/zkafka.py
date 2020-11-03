from confluent_kafka import avro, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import avro, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
import os
import traceback
from datetime import datetime
from dateutil import parser
from . import bugsnagLogger as bugsnag

class Consumer:
    def __init__(self, topic, client_id="client-1", group_id="group-1", config={}):
        self.topic = topic
        schema_path = os.getenv("KAFKA_SCHEMA_PATH")
        scehma_str = ""
        if schema_path:
            with open(os.path.join(schema_path, topic+".json")) as fr:
                scehma_str = fr.read()
                schema = avro.loads(scehma_str)
        else:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "article.json")) as fr:
                scehma_str = fr.read()
                schema = avro.loads(scehma_str)
        if not schema:
            raise Exception("No schema provided!")
        schema_settings = {
            "url": os.getenv("KAFKA_SCHEMA_URL"),
        }
        self.schema_registry_client = SchemaRegistryClient(schema_settings)
        avro_deserializer = AvroDeserializer(scehma_str, self.schema_registry_client, self.deserialize)
        settings = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
            "key.deserializer": StringDeserializer("utf-8"),
            "value.deserializer": avro_deserializer,
            "group.id": group_id,
            "client.id": client_id,
            "enable.auto.commit": bool(os.getenv("KAFKA_AUTOCOMMIT")),
            "session.timeout.ms": int(os.getenv("KAFKA_TIMEOUT_MS")) if os.getenv("KAFKA_TIMEOUT_MS") else 6000,
            "auto.offset.reset": "earliest"
        }
        if not os.getenv("KAFKA_NO_SSL"):
            settings.update({"security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SSL"})
        if config:
            settings.update(config)
        self.client = DeserializingConsumer(settings)
        self.client.subscribe([topic])
        self.last_message = None
        self.config = {
            "raise.uncommitted": bool(os.getenv("KAFKA_RAISE_UNCOMMITED")),
            "auto.commit": bool(os.getenv("KAFKA_COMMIT_PREVOUS"))
        }
        
    def stats_report(self, *args, **kwargs):
        print("iii", args, kwargs)
        
    def deserialize(self, data, ctx):
        if "time" in data:
            if isinstance(data["time"], str):
                try:
                    data["time"] = datetime.fromisoformat(data["time"])
                except:
                    try:
                        data["time"] = parser.parse(data["time"])
                    except:
                        pass
                    
        return data

    def get_data(self):
        while 1:
            msg = None
            try:
                if self.config["auto.commit"]:
                    if self.last_message:
                        self.commit(self.last_message)
                elif self.config["raise.uncommitted"] and self.last_message:
                    raise Exception("Uncommited previous message")
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
                return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                traceback.print_exc()
                bugsnag.notify(msg.error())

    def commit(self, msg):
        self.client.commit(msg)
        if self.last_message == msg:
            self.last_message = None

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
        

class Producer:
    def __init__(self, topic, config={}):
        self.topic = topic
        schema_path = os.getenv("KAFKA_SCHEMA_PATH")
        scehma_str = ""
        if schema_path:
            with open(os.path.join(schema_path, topic+".json")) as fr:
                scehma_str = fr.read()
                schema = avro.loads(scehma_str)
        else:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "article.json")) as fr:
                scehma_str = fr.read()
                schema = avro.loads(scehma_str)
        if not schema:
            raise Exception("No schema provided!")
        schema_settings = {
            "url": os.getenv("KAFKA_SCHEMA_URL"),
        }
        self.schema_registry_client = SchemaRegistryClient(schema_settings)
        avro_serializer = AvroSerializer(scehma_str, self.schema_registry_client, self.serialize)
        settings = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
            "on_delivery": self.delivery_report,
            "key.serializer": StringSerializer('utf-8'),
            "value.serializer": avro_serializer,
            "stats_cb": self.stats_report,
        }
        if config:
            settings.update(config)
        self.client = SerializingProducer(settings)

    def serialize(self, data, ctx):
        keys = ["link", "source", "parentText", "category", "title", "bullets", "fullarticle", "image", "time", "keywords", "language", "parasum"]
        if "articleimagelink" in data:
            data["image"] = data["articleimagelink"]
        if "time" in data:
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
        for key in data:
            if key not in keys:
                del data[key]
        return data

    def stats_report(self, *args, **kwargs):
        print("iii", args, kwargs)

    def delivery_report(self, err, msg):
        if err is not None:
            print("Delivery failed: {}".format(err))
            bugsnag.notify(err)
        else:
            print("Delivered {} [{}]".format(msg.topic(), msg.partition()))
            
    def send_data(self, msg, flush=False):
        self.client.poll(0)
        try:
            self.client.produce(topic=self.topic, value=msg)
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
        fa = self.client.delete_topics([topic])
        if value:
            fs = self.client_schema.delete_subject([topic+"-value"])
        if key:
            fs = self.client_schema.delete_subject([topic+"-key"])
        return fa, fs
