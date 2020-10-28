from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import avro, KafkaError
import os
import traceback

class Consumer:
    def __init__(self, topics, client_id="client-1", group_id="group-1"):
        settings = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
            "group.id": group_id,
            "client.id": client_id,
            "schema.registry.url": os.getenv("KAFKA_SCHEMA_URL"),
            "enable.auto.commit": bool(os.getenv("KAFKA_AUTOCOMMIT")),
            "session.timeout.ms": int(os.getenv("KAFKA_TIMEOUT_MS")) if os.getenv("KAFKA_TIMEOUT_MS") else 6000,
            "default.topic.config": {"auto.offset.reset": "earliest"}
        }
        if not os.getenv("KAFKA_NO_SSL"):
            settings.update({"security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SSL"})
        self.consumer = AvroConsumer(settings)
        self.consumer.subscribe([topics])

    def get_data(self):
        while 1:
            try:
                print("Waiting for data...")
                msg = self.consumer.poll(3)
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
        self.consumer.commit(msg)

    def close(self):
        self.consumer.close()

        

class Producer:
    def __init__(self, topic):
        self.topic = topic
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema", topic+".json")) as fr:
            schema = avro.loads(fr.read())
        self.producer = AvroProducer({
            "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
            "on_delivery": self.delivery_report,
            "schema.registry.url": os.getenv("KAFKA_SCHEMA_URL")
        }, default_value_schema=schema)

    def delivery_report(self, err, msg):
        if err is not None:
            print("Delivery failed: {}".format(err))
            #@TODO SEND TO BUGSNAG
        else:
            print("Delivered {} [{}]".format(msg.topic(), msg.partition()))
            
    def send_data(self, msg, flush=False):
        self.producer.produce(topic=self.topic, value=msg)
        if flush:
            self.producer.flush()

    def flush(self):
        return self.producer.flush()


#@TODO
class Admin:
    def __init__(self):
        pass