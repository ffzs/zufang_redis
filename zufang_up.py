# encoding:utf-8
import random
import requests
import time
from USA import *
import redis
from lxml import etree

if __name__ == '__main__':
    rds = redis.from_url('redis://:lj910226@192.168.3.7:6379',db=1,decode_responses=True)
    base_url = 'https://m.fang.com/zf/?purpose=%D7%A1%D5%AC&jhtype=zf&city=%B1%B1%BE%A9&renttype=cz&c=zf&a=ajaxGetList&city=bj&r=0.743897934517868&page='
    for i in range(1, 3850):
        start_url = base_url + str(i)
        rds.rpush("zufang:start_urls", start_url)

    while True:
        headers = {
            'Referer': 'www.baidu.com',
            'User-Agent': random.choice(USA_phone),
        }
        next_url = rds.lpop("zufang:start_urls")
        print(next_url)
        if next_url:
            response = requests.get(next_url, headers=headers)
            response = etree.HTML(response.text)
            urls = response.xpath('//a[@class="tongjihref"]/@href')
            for url in urls:
                url = "https:" + url
                rds.rpush("zufang:urls",url)
            time.sleep(random.randint(2,4))
        else:
            break

