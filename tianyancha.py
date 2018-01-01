#encoding:utf-8
import threading
import urllib.parse
import time
from bs4 import BeautifulSoup
import requests
import random
from USA import *
import pymongo
import pandas as pd
import redis

client = pymongo.MongoClient("mongodb://ffzs:lj910226@192.168.3.7:2018",connect=False)
db=client["test"]

def save_to_mongo(result):
    if db["company"].insert(result):
        print('存储到MongoDB成功', result)
        return True
    return False

def get_company_infor(content):
    keys = rds.hkeys("hash_xila")
    key = random.choice(keys)
    proxy = rds.hget("hash_xila", key)
    ip = eval(proxy)["ip"]
    proxies = {"http": ip}
    headers = {
        'Referer': 'https://m.tianyancha.com/?jsid=SEM-BAIDU-PZPC-000000',
        'User-Agent': random.choice(USA_phone)
    }
    keyword = urllib.parse.quote(content)
    url = "https://m.tianyancha.com/search?key=" + keyword + "&checkFrom=searchBox"
    try:
        response = requests.get(url=url, headers=headers,proxies = proxies)
        soup = BeautifulSoup(response.text,"lxml")
        if soup.find("a",class_="query_name in-block"):
            name = soup.find("a",class_="query_name in-block").find("span").get_text()
            if soup.find("a",class_="legalPersonName"):
                legalperson =soup.find("a",class_="legalPersonName").get_text()
            else:
                legalperson = ""
            registered_capital = soup.find("div",class_="search_row_new_mobil").find_all("div",class_="title")[1].find("span").get_text()
            registered_time = soup.find("div",class_="search_row_new_mobil").find_all("div",class_="title")[2].find("span").get_text()
            if soup.find("svg"):
                score = soup.find("svg").find("text").get_text()
            else:
                score = ""
            tiany_url = "https://m.tianyancha.com" + soup.find("a",class_="query_name in-block")["href"]

            total = {
                "company":content,
                "get_name": name,
                "legal_person":legalperson,
                "registered_capital": registered_capital,
                "registered_time": registered_time,
                "score": score,
                "url": tiany_url,
            }
            save_to_mongo(total)

    except Exception as e:
        print(content,e,ip,"不可用")
        if key in keys:
            print("*****************删除{}***************".format(key))
            rds.hdel("hash_xila", key)
        get_company_infor(content)
    else:
        print("访问状态：",response.status_code)
        if response.status_code!=200:
            get_company_infor(content)

if __name__ == '__main__':
    rds = redis.from_url('redis://:lj910226@192.168.3.7:6379', db=1, decode_responses=True)
    df = pd.read_csv("address.csv")
    companys = df["company"].tolist()
    for company in companys:
        t1 = threading.Thread(target=get_company_infor, args=[company])
        t1.start()
        time.sleep(0.5)
