# Ziro Kafka

## Environments
Environment | Required | Default | Description
--- | --- | --- | ---
BUGSNAG_APIKEY | **TRUE** | none | Bugsnag API Key
ENABLE_FILE_LOG | false | false | Local file logging
KAFKA_API_KEY | **TRUE** | none | API Key
KAFKA_API_SECRET | **TRUE** | none | API Secret
KAFKA_AUTOCOMMIT | false | false | Kafka autocommit on retrieve message
KAFKA_BROKERS | **TRUE** | none | Broker url
KAFKA_CERT_FILEPATH | false | none | File or directory path to CA certificate(s) for verifying the broker's key
KAFKA_COMMIT_PREVOUS | false | false | Commit last message if uncommitted on next poll
KAFKA_RAISE_UNCOMMITED | false | false | Raise exception if previous message is uncommitted on next poll
KAFKA_SASL_MECHANISM | false | PLAIN | Security Protocol
KAFKA_SCHEMA_API_KEY | **TRUE** | none | Schema Registry API Key
KAFKA_SCHEMA_API_SECRET | **TRUE** | none | Schema Registry API Secret
KAFKA_SCHEMA_PATH | false | *local data* | Custom schema directory named `{topic}.json`
KAFKA_SCHEMA_URL | **TRUE** | none | Schema registry url
KAFKA_SEC_PROTOCOL | false | SASL_SSL | Security Protocol
KAFKA_SSL_PRIV_PATH | false | none | Path to client's private key (PEM) used for authentication.
KAFKA_SSL_PUB_KEY | false | none | Path to client's public key (PEM) used for authentication.
KAFKA_TIMEOUT_MS | false | 6000 | kafka timeout is milliseconds
KAFKA_USE_LOCAL | false | false | **API Keys** will not be required
KAFKA_USE_SSL | false | false | Flag to use SSL

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

BUGSNAG_APIKEY
ENABLE_FILE_LOG
KAFKA_API_KEY
KAFKA_API_SECRET
KAFKA_AUTOCOMMIT
KAFKA_BROKERS
KAFKA_CERT_FILEPATH
KAFKA_COMMIT_PREVOUS
KAFKA_RAISE_UNCOMMITED
KAFKA_SASL_MECHANISM
KAFKA_SCHEMA_API_KEY
KAFKA_SCHEMA_API_SECRET
KAFKA_SCHEMA_PATH
KAFKA_SCHEMA_URL
KAFKA_SEC_PROTOCOL
KAFKA_SSL_PRIV_PATH
KAFKA_SSL_PUB_KEY
KAFKA_TIMEOUT_MS
KAFKA_USE_LOCAL
KAFKA_USE_SSL