import random
import threading
import pymongo
import redis
import requests
import urllib.parse
import time
from USA import *
import pandas as pd


client = pymongo.MongoClient("mongodb://ffzs:lj910226@192.168.3.7:2018",connect=False)
db=client["test"]

def save_to_mongo(result):
    if db["address"].insert(result):
        print('存储到MongoDB成功', result)
        return True
    return False

def get_coordinate(info):
    headers = {
        'Referer': 'http://lbs.amap.com/console/show/picker',
        'User-Agent': random.choice(USA_TRY)
    }
    if len(str(info["address"])) >9:
        content = info["address"]
    else:
        content = info["company"]
    keyword = urllib.parse.quote(content)
    keys = rds.hkeys("hash_xila")
    key = random.choice(keys)
    proxy = rds.hget("hash_xila", key)
    ip = eval(proxy)["ip"]
    proxies = {"http": ip}
    url = "http://restapi.amap.com/v3/place/text?key=8325164e247e15eea68b59e89200988b&keywords="+keyword+"&types=公司&city=北京&children=1&offset=1&page=1&extensions=all"
    try:
        response = requests.get(url=url, headers=headers, proxies=proxies)
        if response.status_code == 200:
            print(response.text)
            if "location" in response.text:
                result = eval(response.text)

                coordinate = result["pois"][0]["location"]
                lon,lat= coordinate.split(",")
                area = result["pois"][0]["adname"]
                tital = {
                    "lon":lon,
                    "lat":lat,
                    "area":area
                }
                save_to_mongo({**info, **tital})
            else:save_to_mongo(info)
        else:
            keys = rds.hkeys("hash_xila")
            if key in keys:
                print("*****************删除{}***************".format(key))
                rds.hdel("hash_xila", key)
            get_coordinate(info)

    except Exception as e:
        print(e)
        keys = rds.hkeys("hash_xila")
        if key in keys:
            print("*****************删除{}***************".format(key))
            rds.hdel("hash_xila", key)
        get_coordinate(info)

if __name__ == '__main__':
    rds = redis.from_url('redis://:lj910226@192.168.3.7:6379', db=1, decode_responses=True)
    # df = pd.read_csv("address.csv")
    # del df["Unnamed: 0"]
    # for i in range(0,len(df)):
    #     info = df.loc[i].to_dict()
    #     rds.lpush("zhilian:address",info)
    while True:
        if rds.rpop("zhilian:address"):
            info = eval(rds.rpop("zhilian:address"))
            t1 = threading.Thread(target=get_coordinate,args=[info])
            t1.start()
            time.sleep(1)
        else:
            break


