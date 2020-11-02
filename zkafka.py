from confluent_kafka import avro
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
from bson import ObjectId
from dateutil import parser

class Consumer:
    def __init__(self, topic, client_id="client-1", group_id="group-1", config={}):
        schema_path = os.getenv("KAFKA_SCHEMA_PATH")
        scehma_str = ""
        if schema_path:
            with open(os.path.join(schema_path, topic+".json")) as fr:
                scehma_str = fr.read()
                schema = avro.loads(scehma_str)
        else:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema", topic+".json")) as fr:
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
        
    def deserialize(self, data, ctx):
        if "_id" in data:
            if isinstance(data["_id"], str):
                data["_id"] = ObjectId(data["_id"])
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
            try:
                print("Waiting for data...")
                msg = self.client.poll(3)
            except SerializerError as e:
                traceback.print_exc()
                #@TODO SEND TO BUGSNAG
                
            if msg is None:
                continue
            elif not msg.error():
                return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                traceback.print_exc()
                #@TODO SEND TO BUGSNAG

    def commit(self, msg):
        self.client.commit(msg)

    def close(self):
        self.client.close()

        

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
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema", topic+".json")) as fr:
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
        if "_id" in data:
            data["_id"] = str(data["_id"])
        if "time" in data:
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
        return data

    def stats_report(self, *args, **kwargs):
        print("iii", args, kwargs)

    def delivery_report(self, err, msg):
        if err is not None:
            print("Delivery failed: {}".format(err))
            #@TODO SEND TO BUGSNAG
        else:
            print("Delivered {} [{}]".format(msg.topic(), msg.partition()))
            
    def send_data(self, msg, flush=False):
        self.client.poll(0)
        self.client.produce(topic=self.topic, value=msg)
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

    def new_topic(self, topic, partition, **kwargs):
        return self.client.create_topics([NewTopic(topic, partition, **kwargs)])
    
    def new_parition(self, topic, partition):
        return self.client.create_partitions([NewPartitions(topic, partition)])

class SchemaReg:
    def __init__(self):
        settings = {
            "url": os.getenv("KAFKA_SCHEMA_URL"),
        }
        self.client = SchemaRegistryClient(settings)
        
    def get_subjects(self):
        return self.client.get_subjects()

    def get_versions(self, subject):
        return self.client.get_versions(subject)

    def get_schema_version(self, subject, version):
        return self.client.get_version(subject, version)

    def schema_upsert(self, subject, schema_str, schema_type="AVRO"):
        schema = Schema(schema_str=schema_str, schema_type=schema_type)
        return self.client.register_schema(subject_name=subject, schema=schema)