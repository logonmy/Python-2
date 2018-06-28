#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests, random, lxml, json, time
from bs4 import BeautifulSoup
from kafka import TopicPartition
from utils import kafka_client, file_utils, browser_useragents_utils, redis_client, config

"""
    爬取ei列表设计思路：
        1、获取searchid
        2、设置每一页列表条数
        3、根据分页爬取列表后精简数据并发送至kafka
            如果爬取失败，程序随机暂停2至5秒后接着进行重试，重试超过五次，程序自动退出。
            并写入爬取进度以及失败原因至redis，供爬虫管理系统可视化使用
"""
        #TODO 4、发送至kafka的同时并且保存到本地(开发测试方便，完成后需要删除)

# 本程序采用灵活可配置方式，暂时采用统一配置文件key，value形式，待爬虫管理系统研发完成，配置方式统一迁入管理库
class SpiderMain(object):

    # 列表爬虫类构造函数：用于初始化程序，从配置文件config.py中获取配置，用于初始化kafka集群，redis，mysql，爬虫参数等
    def __init__(self):
        self.exportvalue = config.exportvalue
        # kafka初始化
        self.kafka_client = kafka_client.KafkaClient()
        self.producer = self.kafka_client.get_producer()
        self.topic = config.kafka_topic
        # redis初始化
        self.redis_client = redis_client.RedisClient().get_connection()
        # 由于ei网站分页方式不同，因此循环页数并不参与分页查询
        # page_count = page_count + 25 ,第一页的25条分页参数为1，第二页为26，第三页为51
        self.page_count = 1
        self.page_size = 1000
        self.current_page = 1
        # loop_num获取列表失败时使用，每次加一，尝试超过5次就放弃跳出循环
        self.loop_num = 0
        self.max_loop_num = 50

    def base_url(self):
        session = requests.session()
        root = "https://www.engineeringvillage.com/search/expert.url"
        headers = {
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
        }
        response = session.get(root)
        cookies = requests.utils.dict_from_cookiejar(session.cookies)
        print(response.status_code)
        response.close()
        if response.status_code == 200:
            return headers, cookies
        return None,None

    # 获取url，因为url中包含了302重定向的地址
    def get_url(self):
        time.sleep(random.uniform(1, 3000) / 1000)
        headers, cookies = self.base_url()
        if headers is not None:
            session = requests.session()
            root = "https://www.engineeringvillage.com/search/submit.url?usageOrigin=searchform&usageZone=expertsearch&editSearch=&isFullJsonResult=true&angularReq=true&CID=searchSubmit&searchtype=Expert&origin=searchform&category=expertsearch&searchWord1=" + self.exportvalue + "&database=1&yearselect=yearrange&startYear="+config.startYear+"&endYear="+config.endYear+"&updatesNo=1&sort=relevance&autostem=true"
            response = session.get(root, headers=headers, cookies=cookies, allow_redirects=False)
            cookies = requests.utils.dict_from_cookiejar(session.cookies)
            print(response.status_code)
            if response.status_code == 302:
                session = requests.session()
                response = session.get(response.headers["location"], headers=headers, cookies=cookies)
                cookies = requests.utils.dict_from_cookiejar(session.cookies)
                #print(response)
                response.close()
                return response.url,headers, cookies
            response.close()
        return None

    # 设置每页为100条
    def set_pagesize(self, url,headers, cookies):
        time.sleep(random.uniform(1, 3000) / 1000)
        SEARCHID = file_utils.url_parse(url, "SEARCHID")
        root = "https://www.engineeringvillage.com/search/results/expert.url?pageSizeVal="+str(self.page_size)+"&SEARCHID=" + str(SEARCHID) + "&sort=yr&sortdir=dw&angularReq=true&isFullJsonResult=false&usageOrigin=searchresults&usageZone=resultsperpagetop"
        response = requests.get(root, headers=headers, cookies=cookies)
        if response.status_code == 200:
            #return SEARCHID, headers, cookies, file_utils.url_parse(response.url, "_"), response.json().get("pagenav").get("nextindex"), response.json().get("pagenav").get("resultscount")
            return SEARCHID, response.headers, cookies, response.json().get("searchMetaData").get("searchesEntity").get("savedate"), response.json().get("pagenav").get("nextindex"), response.json().get("pagenav").get("resultscount")
        response.close()
        return None

    def get_list_project(self,searchid,page_count,timestamp,headers,cookies):
        time.sleep(random.uniform(1, 3000) / 1000)
        results_list = []
        data_url = "https://www.engineeringvillage.com/search/results/expert.url?navigator=NEXT&SEARCHID=" + searchid + "&database=1&angularReq=true&isFullJsonResult=false&usageOrigin=searchresults&COUNT=" + str(page_count) + "&usageZone=nextpage&_=" + str(int(timestamp) + 1)
        response = requests.get(data_url, headers=headers, cookies=cookies)
        #print(response.text)
        json = response.json()
        navigators = json.get("navigators")
        newNavigators = json.get("newNavigators")
        numericalData = json.get("numericalData")
        pagenav = json.get("pagenav")
        results = json.get("results")
        if len(results) > 0:
            for item in results:
                title = item.get("title")
                accession_number = item.get("accnum")
                abstracthref = item.get("abstracthref")
                abstractlink = item.get("abstractlink")
                authors = item.get("authors")
                # 论文相关信息，比如id之类的
                doc = item.get("doc")
                # doi/isbn/issn/pii、、、、
                citedby = item.get("citedby")
                results_list.append({"docindex": doc.get("hitindex"),"accession_number":accession_number, "docid": doc.get("docid"), "title": title})
        searchMetaData = json.get("searchMetaData")
        sortOptions = json.get("sortOptions")

        return results_list

    # 程序入口
    def craw1(self):
        try:
            url, headers, cookies = self.get_url()
            if url is not None:
                searchid, headers, cookies, timestamp, page_count, total_num = self.set_pagesize(url, headers=headers, cookies=cookies)
                #total_num = 67478
                total_page = int(total_num / self.page_size) + 1
                # page_count = 1
                while self.current_page <= total_page:
                    print("第%s页开始爬取，爬取%d条数据" % (self.current_page, self.page_size))
                    list_data = self.get_list_project(searchid, self.page_count, timestamp, headers, cookies)
                    print(list_data)
                    if len(list_data) == 0:
                        print("开始为空了。。。。。。")
                    self.page_count = self.page_count + self.page_size
                    # 数据写入文件
                    file_utils.write_to_file(list_data, "crawler_ei_list_main.txt")
                    print("爬取第%s页数据，并且发送至kafka成功！" % self.current_page)
                    print("--------------------忧伤的分割线----------------------")
                    self.current_page += 1
        except:
            time.sleep(10)
            self.loop_num = self.loop_num + 1
            if self.loop_num == self.max_loop_num:
                return
            self.craw1()

    # 程序入口
    def craw(self):
        try:
            url, headers, cookies = self.get_url()
            if url is not None:
                searchid, headers, cookies, timestamp, page_count, total_num = self.set_pagesize(url, headers=headers, cookies=cookies)
                total_page = int(total_num / self.page_size) + 1
                #page_count = 1
                while self.current_page <= total_page:
                    print("第%s页开始爬取，爬取%d条数据" % (self.current_page, self.page_size))
                    list_data = self.get_list_project(searchid, page_count, timestamp, headers, cookies)
                    if len(list_data) ==0:
                        print("开始为空了。。。。。。")
                    #print(list_data)
                    partitions = len(self.producer.partitions_for(self.topic))
                    if len(list_data) > 0:
                        for index in range(0, len(list_data)):
                            partition = index % partitions
                            future = self.producer.send(topic=self.topic, value=json.dumps(list_data[index]).encode('utf-8'), key=None, partition=int(list_data[index]["docindex"]) % partitions, timestamp_ms=None)
                            recordMetadata = future.get()
                            # print("topic:%s,partition:%s,offset:%s" % (recordMetadata.topic, recordMetadata.partition, recordMetadata.offset))
                            # set
                            self.redis_client.sadd(config.redis_list_name + "set_bak", json.dumps(list_data[index]))
                            # list
                            self.redis_client.lpush(config.redis_list_name + "list_bak", json.dumps(list_data[index]))
                    self.page_count = self.page_count + self.page_size
                    # 数据写入文件
                    #file_utils.write_to_file(list_data, "crawler_ei_list_main.txt")
                    print("爬取第%s页数据，并且发送至kafka成功！" % self.current_page)
                    print("--------------------忧伤的分割线----------------------")
                    self.current_page += 1
        except:
            time.sleep(10)
            self.loop_num = self.loop_num + 1
            if self.loop_num == self.max_loop_num:
                return
            self.craw()

# 爬虫启动入口
if __name__ == "__main__":
    spiderMain = SpiderMain()
    spiderMain.craw()

