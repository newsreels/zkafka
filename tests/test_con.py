import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from zkafka import Consumer
import sys
import time
import random

cid = sys.argv[1]

con = Consumer("ktest1", client_id=cid, group_id="test_group")
while 1:
    x = con.get_data()
    print(">>", x.value())
    time.sleep(random.randint(3,6))