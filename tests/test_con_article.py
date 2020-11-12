import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from zkafka import Consumer
import sys
import time
import random

topic = sys.argv[1]

con = Consumer(topic, client_id="con-1", group_id="art_group")
while 1:
    try:
        x = con.get_data()
        sleep = random.randint(1,3)
        print(x.value()['title'])
        print(">>", x.value()['title'],"\n", x.partition(), "  sleep:", sleep)
        time.sleep(sleep)
        con.commit(x)
    except KeyboardInterrupt:
        break
