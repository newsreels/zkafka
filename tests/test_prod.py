import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import traceback
from zkafka import Producer

total = 10
if len(sys.argv) > 1:
    try:
        print(sys.argv[1])
        total = int(sys.argv[1])
    except:
        traceback.print_exc()
prod = Producer('ktest1')

print(">>", total)
for x in range(total):
    prod.send_data({
        "title": "mydata",
        "value": x+1
    })
prod.flush()
print("Data sent")