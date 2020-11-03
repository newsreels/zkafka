import os
import sys
_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
print("ROOT", _root)
sys.path.insert(0, _root)
import traceback
from zkafka import Producer
from pymongo import MongoClient

mongo = MongoClient()
db = mongo.articles_db
articles = db.articles

total = 1
pipe = "6"
if len(sys.argv) > 1:
    try:
        print(sys.argv[1])
        pipe = sys.argv[1]
        total = int(sys.argv[2])
    except:
        traceback.print_exc()
prod = Producer('pipe3')

print(">>", pipe)
for art in articles.aggregate([ { '$match': { 'pipelineStatus': pipe } }, { '$sample': { 'size': total } } ]):
    print(">>", art["_id"])
    print(">>", art["title"])
    try:
        prod.send_data(art)
    except Exception as e:
        print("\n\n")
        print("!!!", art["_id"])
        print("\n\n")
        raise e
prod.flush()
print("Data sent")
