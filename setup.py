import setuptools
import os

# Package meta-data
NAME = 'zkafka'
DESCRIPTION = 'Simple Kafka Avro Consumers and Producers for Ziro Pipeline'
URL = 'https://github.com/ziroride/zkafka'
EMAIL = 'alfred@newsinbullets.com'
AUTHOR = 'Alfred Ray Jayag'
VERSION = '1.2.0'

REQUIRED = [
    'confluent-kafka[avro]',
    'python-dateutil',
    'bugsnag'
]

here = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(here, "README.md"), "r") as fh:
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
    python_requires='>=3.6',
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src', include=['zkafka']),
    package_data = {'': ['data/article.json']},
    install_requires=REQUIRED,
    include_package_data=True,
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)