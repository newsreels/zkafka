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


class BaseClient:
    def __init__(self, topic):
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

        self._schema_str = scehma_str
        self._client_settings = settings
        self.schema_registry_client = SchemaRegistryClient(schema_settings)

class Consumer(BaseClient):
    def __init__(self, topic, client_id="client-1", group_id="group-1", config={}, verbose=False):
        super().__init__(topic)
        self.topic = topic
        self.verbose = verbose
        avro_deserializer = AvroDeserializer(self._schema_str, self.schema_registry_client, self.deserialize)
        settings = {
            "key.deserializer": StringDeserializer("utf-8"),
            "value.deserializer": avro_deserializer,
            "group.id": group_id,
            "client.id": client_id,
            "enable.auto.commit": bool(os.getenv("KAFKA_AUTOCOMMIT")),
            "session.timeout.ms": int(os.getenv("KAFKA_TIMEOUT_MS")) if os.getenv("KAFKA_TIMEOUT_MS") else 6000,
            "auto.offset.reset": "earliest"
        }
            
        if config:
            settings.update(config)

        settings.update(self._client_settings)
        self.client = DeserializingConsumer(settings)
        self.client.subscribe([topic])
        self.last_message = None
        self.config = {
            "raise.uncommitted": bool(os.getenv("KAFKA_RAISE_UNCOMMITED")),
            "auto.commit": bool(os.getenv("KAFKA_COMMIT_PREVOUS"))
        }
        
    def stats_report(self, *args, **kwargs):
        if self.verbose:
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

    def get_data(self, value=False):
        wait = 1
        while 1:
            msg = None
            try:
                if self.config["auto.commit"]:
                    if self.last_message:
                        self.commit(self.last_message)
                elif self.config["raise.uncommitted"] and self.last_message:
                    raise Exception("Uncommited previous message")
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
                    return msg.value()
                else:
                    return msg
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                wait = 1
                pass
            else:
                wait = 1
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
        

class Producer(BaseClient):
    def __init__(self, topic, config={}, verbose=False):
        super().__init__(topic)
        self.topic = topic
        self.verbose = verbose
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
        self.client = SerializingProducer(settings)

    def serialize(self, data, ctx):
        keys = ["link", "source", "parentText", "category", "title", "bullets", "fullarticle", "image", "time", "keywords", "language", "parasum"]
        if "articleimagelink" in data:
            data["image"] = data["articleimagelink"]
        if "time" in data:
            if isinstance(data["time"], datetime):
                data["time"] = data["time"].isoformat()
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
