#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests, random, lxml, json, time
from bs4 import BeautifulSoup
from kafka import TopicPartition
from utils import kafka_client, file_utils, browser_useragents_utils, redis_client, config

"""
    爬取wos列表设计思路：
        1、获取sId
        2、根据sId获取prId
        3、根据分页爬取列表后精简数据并发送至kafka
            如果爬取失败，程序随机暂停2至5秒后接着进行重试，重试超过五次，程序自动退出。
            并写入爬取进度以及失败原因至redis，供爬虫管理系统可视化使用
"""
        #TODO 4、发送至kafka的同时并且保存到本地(开发测试方便，完成后需要删除)

# 本程序采用灵活可配置方式，暂时采用统一配置文件key，value形式，待爬虫管理系统研发完成，配置方式统一迁入管理库
class SpiderMain(object):

    # 列表爬虫类构造函数：用于初始化程序，从配置文件config.py中获取配置，用于初始化kafka集群，redis，mysql，爬虫参数等
    def __init__(self):
        self.base_url = 'http://apps.webofknowledge.com'
        self.inputvalue = config.wos_organization
        # kafka初始化
        self.kafka_client = kafka_client.KafkaClient()
        self.producer = self.kafka_client.get_producer()
        self.topic = config.kafka_topic
        # redis初始化
        self.redis_client = redis_client.RedisClient().get_connection()
        self.page_size = 50
        self.current_page =1
        # loop_num获取列表失败时使用，每次加一，尝试超过5次就放弃跳出循环
        self.loop_num = 0
        self.max_loop_num = 300
        """
            需要检索的放开对应
                self.product = ''
                self.general_url = ''
            即可。
                product：代表web of science网站中三种类型库的识别代码。
                general_url:选好检索条件，进行检索时生成prid的url地址.

            光修改此处还无法进行列表下载，还需进行get_prid方法中的
                self.form_prid_data 进行修改，具体根据不同情况进行适配，数据来源于网页中具体请求的request formdata值
                request formdata值暂时未确定，文老师正在整理他们需要的规则
        """
        # Web of Science核心集合
        self.product = 'WOS'
        self.general_url = 'WOS_GeneralSearch.do'

        # 中国科学引文数据库 SM
        # self.product = 'CSCD'
        # self.general_url = 'CSCD_GeneralSearch.do'

        # MEDLINE
        # self.product = 'MEDLINE'
        # self.general_url = 'MEDLINE_GeneralSearch.do'

    # 获取sid
    def get_sid(self, ):
        root = 'http://www.webofknowledge.com/'
        response = requests.get(root, headers={'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS)})
        sid = file_utils.url_parse(response.url, "SID")
        response.close()
        return sid

    # 获取pid
    def get_prid(self, sid):
        root_url = self.base_url + '/' + self.general_url
        self.form_prid_hearders = {
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS)
        }
        # 所有年份
        self.form_prid_data = {
            "action": "search",
            "product": self.product,
            "search_mode": "GeneralSearch",
            "SID": sid,
            # "sa_params": self.product + "||" + sid + "|http://apps.webofknowledge.com|'",
            "value(input1)": self.inputvalue,
            "value(select1)": "OG",
            "startYear": config.startYear,
            "endYear": config.endYear
        }
        s = requests.Session()
        response = s.post(root_url, data=self.form_prid_data, headers=self.form_prid_hearders)
        prId = file_utils.url_parse(response.url, "prID")
        # 获取总页数
        total_num = int(BeautifulSoup(response.text, "lxml").find(id="trueFinalResultCount").text)
        response.close()
        return prId, total_num

    """
        获取每一页的数据
        抓取数据，参数：当前第几页，每页多少条
    """

    def get_page_html(self, sid, current_page, page_size):
        params = {}
        headers = {
            "User-Agent": random.choice(browser_useragents_utils.USER_AGENTS)
        }
        url = "http://apps.webofknowledge.com/summary.do?product=" + self.product + "&parentProduct=" + self.product + "&search_mode=GeneralSearch&qid=1&SID=" + sid + "&page=" + str(current_page) + "&action=changePageSize&pageSize=" + str(page_size)
        response = requests.get(url, headers)
        soup = BeautifulSoup(response.text, "lxml")
        response.close()
        return soup

    # 获取sId，totalNum(总页数)
    def get_sId_prId_totleNum(self):
        print("获取sId和prId开始、、、、、、、、、、、")
        sId = self.get_sid()
        print("sId=", sId)
        prId, total_num = self.get_prid(sId)
        print("prId=", prId)
        return sId, total_num

    # 程序入口
    def craw(self):
        # 循环获取每一页的列表数据，并且分批发送至kafka。
        try:
            sId, total_num = self.get_sId_prId_totleNum()
            if total_num > 0:
                # 默认第一页开始
                total_page = int(total_num / self.page_size) + 1
                while self.current_page <= total_page:
                    print("第%s页开始爬取，爬取%d条数据" % (self.current_page, self.page_size))
                    # 循环抓取数据
                    list_data = self.get_list_project(sId, self.current_page, self.page_size)
                    """
                        list_data数据列表发送至kafka
                        发送条件：1、爬取列表数据，根据设置：10，25,50条，建议使用50条/页，尽量最高效率爬取数据。
                                 2、每一条列表数据写入kafka topic
                                 3、每一条列表数据写入文件作为暂时对比数据
                    """
                    if len(list_data) > 0:
                        # 1、把list分为长度为10的n段 分别发送给kafka
                        # 2、把list的每一条数据追加写入文件
                        # 3、写入redis
                        partitions = len(self.producer.partitions_for(self.topic))
                        for index in range(0, len(list_data)):
                            """获取topic的partition数量，用来进行数据的负载均衡
                                负载方式：采用轮训的方式
                                    partition = index % partitions
                                    比如有三个parition。那么partition的索引就应该是 0、1、2;0、1、2;0、1、2 """
                            # 数据写入kafka
                            future = self.producer.send(topic=self.topic, value=json.dumps(list_data[index]).encode('utf-8'), key=None, partition=int(list_data[index]["thesis_num"]) % partitions, timestamp_ms=None)
                            """RecordMetadata data type:
                                for exapmple
                                      RecordMetadata(
                                        topic='wos_list1', partition=0, topic_partition=TopicPartition(topic='wos_list1', partition=0), 
                                        offset=5467, timestamp=1525871078418, checksum=None, serialized_key_size=-1, serialized_value_size=199)"""
                            recordMetadata = future.get()
                            # print("topic:%s,partition:%s,offset:%s" % (recordMetadata.topic, recordMetadata.partition, recordMetadata.offset))
                            # set
                            self.redis_client.sadd("crawler_wos_list_set_bak", json.dumps(list_data[index]))
                            # list
                            self.redis_client.lpush("crawler_wos_list_list_bak", json.dumps(list_data[index]))

                        # 数据写入文件
                        #file_utils.write_to_file(list_data, "crawler_wos_list_main.txt")

                    print("爬取第%s页数据，并且发送至kafka成功！" % self.current_page)
                    print("--------------------忧伤的分割线----------------------")
                    self.current_page += 1
        except Exception as e:
            time.sleep(10)
            self.loop_num = self.loop_num + 1
            if self.loop_num == self.max_loop_num:
                return
            self.craw()

    """
        把获取到的每一页数据进行解析返回为列表[{},{},{}、、、]
        获取html，循环简析<div class="search-results">下的所有直接子节点
        参数; 当前第几页，每页多少条
        返回：list集合
    """

    def get_list_project(self, sid, current_page, page_size):
        list_project = []
        soup = self.get_page_html(sid, current_page, page_size)
        item_list = soup.find_all("div", class_='search-results-item')
        for list in item_list:
            # reference_num = int(list.find("div", class_="search-results-number-align").contents[0].replace(".", ""))
            # 序号部分
            thesis_num = list.find("div", class_="search-results-number-align").contents[0].replace(".", "").replace(",", "").strip()
            # 内容直接子节点(包含title、thesis_url，thesis_authors)
            recursive_child = list.find("div", class_="search-results-content").find_all("div", recursive=False)
            thesis_title = recursive_child[0].find("a").find("value").get_text().replace('"', '')
            thesis_url = recursive_child[0].find("a")["href"]
            thesis_authors = recursive_child[1].contents[2]
            tmp_list = {
                "thesis_num": thesis_num,
                "thesis_authors": thesis_authors,
                "thesis_title": thesis_title
            }
            list_project.append(tmp_list)

        return list_project

# 爬虫启动入口
if __name__ == "__main__":
    spiderMain = SpiderMain()
    spiderMain.craw()
