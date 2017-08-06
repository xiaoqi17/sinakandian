# -*- coding: utf-8 -*-
import json
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from urllib import urlencode
import pymongo
import requests
import time
from requests import ConnectionError
import sys
from bs4 import BeautifulSoup
from config import *
import re
reload(sys)
sys.setdefaultencoding('utf-8')


client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]

def sinak_index(headers,cstart):
    data={
        'type': 'ent',
        #'callback': 'jQuery111208126477017872906_1500447808607',  #去掉callback，因为因为jQueryxxxxx 那些都是动态生成的
        'page': cstart,
        'size': '15',
        '_': '1500447808608'
    }
    params = urlencode(data)  # 利用urllib中的urlencode来构建data
    url = 'http://o.mpapi.sina.cn/article/listent'
    urls = url+'?'+ params
    try:
        response = requests.get(urls,headers)
        time.sleep(3)
        response.encoding = response.apparent_encoding  # 编码改为UTF-8
        if response.status_code == 200:  #检测返回码是否200
            return response.text
        return None
    except ConnectionError:
        print('Error occurred')
        return None

def sinak_page_index(text):
    try:
        data = json.loads(text)  #用loads来解析json
        articles=data['result']['data']['articles']  #一层一层提取字典里keys()值
        for i in articles:
            pub_url = i['pub_url']
            print pub_url
            if db[MONGO_TABLE].find_one({'pub_url':pub_url}):  #url去重，如果存在，提示爬过，否则else。
                print '这url爬过'
            else:
                yield pub_url
    except:
        pass

def sinak_content(pub_url,headers):
    try:
        response = requests.get(pub_url,headers)
        time.sleep(3)
        response.encoding = response.apparent_encoding  # 编码改为UTF-8
        if response.status_code == 200:
            soups = re.sub('<span style="font-family: KaiTi_GB2312,KaiTi;font-size:14px;">(.*?)</span>','',response.text)
            soup = BeautifulSoup(soups,'lxml')
            title = soup.select('#artibodyTitle')
            pub_date = soup.select('#pub_date')
            author = soup.select('div.artInfo > div:nth-of-type(1) > span.author > a')
            artibodys  = soup.select('#artibody')
            for title,pub_date,author,artibodys in zip(title,pub_date,author,artibodys):   #利用zip()函数一并处理
                return {
                    'title' : title.get_text(),
                    'pub_date' : pub_date.get_text(),
                    'author' : author.get_text(),
                    'artibodys' : artibodys.get_text(),
                    'pub_url':pub_url
                }

        return None
    except ConnectionError:
        print('Error occurred')
        return None

'''保存本地文件txt,csv,json格式的方式'''
# def write_to_file(content):
#     with open('sinakandian.txt', 'a', ) as f:
#         f.write(json.dumps(content, ensure_ascii=False) + '\n')  #关闭写入的文件中编码
#         f.close()

'''保存mongodb'''
def save_to_mongo(datail):
    if db[MONGO_TABLE].insert(datail):
        print('Successfully Saved to Mongo', datail)
        return True
    return False

def main(cstart):
    headers = {
        'User - Agent': 'Mozilla / 5.0(Windows NT 6.1;WOW64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 59.0.3071.86 Safari / 537.36'
    }

    text = sinak_index(headers, cstart)
    for pub_url in sinak_page_index(text):
        datail = sinak_content(pub_url, headers)
        if datail == None:  #判断数据是否为None
            pass
        else:
            print datail
            # write_to_file(datail)  #保存本地文件
            if datail: save_to_mongo(datail)  #保存mongodb


if __name__ == '__main__':
    pool = Pool()   #默认线程数
    # pool = ThreadPool(16)   #指定线程数
    groups = ([x for x in range(GROUP_START, GROUP_END + 1)])  #构建多线程
    pool.map(main, groups)
    pool.close()
    pool.join()
