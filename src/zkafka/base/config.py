import os

def config():
    settings = {
        "bootstrap.servers": os.getenv("KAFKA_BROKERS")
    }
    if os.getenv("KAFKA_USE_SSL"):
        settings.update({
            "security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "ssl.ca.location": os.getenv("KAFKA_CERT_FILEPATH") or "probe", #/usr/local/etc/openssl/cert.pem
            "sasl.username": os.getenv("KAFKA_API_KEY") if os.getenv("KAFKA_API_KEY") else '', #<api-key>
            "sasl.password": os.getenv("KAFKA_API_SECRET")if os.getenv("KAFKA_API_SECRET") else '', #<api-secret>
        })
    elif not os.getenv("KAFKA_USE_LOCAL"):
        settings.update({
            "security.protocol": os.getenv("KAFKA_SEC_PROTOCOL") or "SASL_SSL",
            "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM") or "PLAIN",
            "sasl.username": os.getenv("KAFKA_API_KEY") if os.getenv("KAFKA_API_KEY") else '', #<api-key>
            "sasl.password": os.getenv("KAFKA_API_SECRET")if os.getenv("KAFKA_API_SECRET") else '', #<api-secret>
        })
    
    return settings