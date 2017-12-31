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
    key = random.choice(keys)
    proxy = rds.hget("hash_kuai",key)
    ip = eval(proxy)["ip"]
    proxies = {"http":ip}
    response = requests.get(url,headers=headers,proxies=proxies)
    try:
        response = etree.HTML(response.text)
        item = {}
        item["title"] = response.xpath('//*[@class="xqCaption mb8"]/h1/text()')[0]
        item["area"] = response.xpath('//*[@class="xqCaption mb8"]/p/a[2]/text()')[0]
        item["location"] = response.xpath('//*[@class="xqCaption mb8"]/p/a[3]/text()')[0]
        item["housing_estate"] = response.xpath('//*[@class="xqCaption mb8"]/p/a[1]/text()')[0]
        item["rent"] = response.xpath('//*[@class="f18 red-df"]/text()')[0]
        item["rent_type"] = response.xpath('//*[@class="f12 gray-8"]/text()')[0][1:-1]
        item["floor_area"] = response.xpath('//*[@class="flextable"]/li[3]/p/text()')[0][:-2]
        item["house_type"] = response.xpath('//*[@class="flextable"]/li[2]/p/text()')[0]
        item["floor"] = response.xpath('//*[@class="flextable"]/li[4]/p/text()')[0]
        item["orientations"] = response.xpath('//*[@class="flextable"]/li[5]/p/text()')[0]
        item["decoration"] = response.xpath('//*[@class="flextable"]/li[6]/p/text()')[0]
        item["house_info"] = response.xpath('//*[@class="xqIntro"]/p/text()')[0]
        item["house_tags"] = ",".join(response.xpath('//*[@class="stag"]/span/text()'))
        save_to_mongo(item)

    except Exception as e:
        print(e)
        if response.status_code !=404:
            rds.hdel("hash_kuai", key)
            get_item(url)

if __name__ == '__main__':
    rds = redis.from_url('redis://:lj910226@192.168.3.7:6379', db=1, decode_responses=True)
    while True:
        url = rds.rpop("zufang:urls")
        print(url)
        if url:
            t1 = threading.Thread(target=get_item,args=[url])
            t1.start()
            time.sleep(0.5)
        else:
            break