# encoding:utf-8
import threading
import pymongo
from USA import *
import random
import requests
import time
import redis
from lxml import etree

client = pymongo.MongoClient("mongodb://ffzs:lj910226@192.168.3.7:2018",connect=False)
db=client["test"]

def save_to_mongo(result):
    if db["zufang"].insert(result):
        print('存储到MongoDB成功', result)
        return True
    return False

def get_item(url):
    headers = {
        'Referer': 'https://m.fang.com/zf/bj/?jhtype=zf',
        'User-Agent': random.choice(USA_TRY),
    }
    keys = rds.hkeys("hash_kuai")
    print(url,len(keys))
    key = random.choice(keys)
    proxy = rds.hget("hash_kuai",key)
    ip = eval(proxy)["ip"]
    proxies = {"http":ip}
    response = requests.get(url,headers=headers,proxies=proxies)
    try:
        selector = etree.HTML(response.content.decode("gbk"))
        item = {}
        item["title"] = selector.xpath('//*[@class="xqCaption mb8"]/h1/text()')[0]
        item["area"] = selector.xpath('//*[@class="xqCaption mb8"]/p/a[2]/text()')[0]
        item["location"] = selector.xpath('//*[@class="xqCaption mb8"]/p/a[3]/text()')[0]
        item["housing_estate"] = selector.xpath('//*[@class="xqCaption mb8"]/p/a[1]/text()')[0]
        item["rent"] = selector.xpath('//*[@class="f18 red-df"]/text()')[0]
        item["rent_type"] = selector.xpath('//*[@class="f12 gray-8"]/text()')[0][1:-1]
        item["floor_area"] = selector.xpath('//*[@class="flextable"]/li[3]/p/text()')[0][:-2]
        item["house_type"] = selector.xpath('//*[@class="flextable"]/li[2]/p/text()')[0]
        item["floor"] = selector.xpath('//*[@class="flextable"]/li[4]/p/text()')[0]
        item["orientations"] = selector.xpath('//*[@class="flextable"]/li[5]/p/text()')[0]
        item["decoration"] = selector.xpath('//*[@class="flextable"]/li[6]/p/text()')[0]
        if selector.xpath('//*[@class="xqIntro"]/p/text()'):
            item["house_info"] = selector.xpath('//*[@class="xqIntro"]/p/text()')[0]
        else:
            item["house_info"] = ""
        item["house_tags"] = ",".join(selector.xpath('//*[@class="stag"]/span/text()'))
        save_to_mongo(item)

    except Exception as e:
        print(e,response.status_code)
        if response.status_code !=200:
            # rds.hdel("hash_kuai", key)
            get_item(url)

if __name__ == '__main__':
    rds = redis.from_url('redis://:lj910226@192.168.3.7:6379', db=1, decode_responses=True)
    while True:
        url = rds.rpop("zufang:urls")
        if url:
            t1 = threading.Thread(target=get_item,args=[url])
            t1.start()
            time.sleep(0.3)
        else:
            break