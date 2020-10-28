import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from zkafka import Producer

prod = Producer('ktest1')

for x in range(100):
    prod.send_data({
        "title": "mydata",
        "value": x+1
    })
prod.flush()
print("Data sent")