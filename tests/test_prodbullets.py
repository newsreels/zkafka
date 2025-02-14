import os
import sys
_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
print("ROOT", _root)
sys.path.insert(0, _root)
import traceback
from zkafka import Producer
from pymongo import MongoClient
import re
import code

mongo = MongoClient()
db = mongo.articles_db
articles = db.articles

total = 1
pipe = "6"
topic = "prod_pipe"
if len(sys.argv) > 1:
    try:
        print(sys.argv[1])
        topic = sys.argv[1]
        pipe = sys.argv[2]
        total = int(sys.argv[3])
    except:
        traceback.print_exc()
prod = Producer(topic)

print(">>", topic, pipe)
for art in articles.aggregate([ { '$match': { 'bullets': re.compile(r"^[\S]+$") } }, { '$sample': { 'size': total } } ]):
    print(">>", art["_id"])
    print(">>", art["title"])
    print(art.keys())
    try:
        prod.send_data(art)
    except Exception as e:
        print("\n\n")
        print("!!!", art["_id"])
        print("\n\n")
        code.interact(local=locals())
        raise e
prod.flush()
print("Data sent")
