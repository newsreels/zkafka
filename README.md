# Ziro Kafka

## Environments
Environment | Required | Default | Description
--- | --- | --- | ---
KAFKA_BROKERS | **true** | none | Broker url
KAFKA_SCHEMA_URL | **true** | none | Schema registry url
KAFKA_SCHEMA_PATH | false | `local data` | Custom schema directory named `{topic}.json`
KAFKA_AUTOCOMMIT | false | false | Kafka autocommit on retrieve message
KAFKA_TIMEOUT_MS | false | 6000 | kafka timeout is milliseconds
KAFKA_NO_SSL | false | true | Flag to use SSL
KAFKA_SEC_PROTOCOL | false | SSL | Security Protocol
KAFKA_RAISE_UNCOMMITED | false | false | Raise exception if previous message is uncommitted on next poll
KAFKA_COMMIT_PREVIOUS | false | false | Commit last message if uncommitted on next poll

## Usage
### zkafka.Consumer
```
consumer = zkafka.Consumer('mytopic', client_id='client.id', group_id='group.id')
while 1:
    x = consumer.get_data()
    ...
```
### zkafka.Producer
```
producer = zkafka.Producer('mytopic')
producer.send_data({"key": "value"})
producer.flush()
```
### zkafka.Admin
```
admin = zkafka.Admin()
admin.get_subjects()
```