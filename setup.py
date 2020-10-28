import setuptools

# Package meta-data
NAME = 'zkafka'
DESCRIPTION = 'Simple Kafka Avro Consumers and Producers for Ziro Pipeline'
URL = 'https://github.com/ziroride/zkafka'
EMAIL = 'alfred@newsinbullets.com'
AUTHOR = 'Alfred Ray Jayag'
VERSION = '0.0.1'

REQUIRED = [
    'confluent-kafka[avro]'
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=URL,
    py_modules=['zkafka'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires='>=3.6',
)