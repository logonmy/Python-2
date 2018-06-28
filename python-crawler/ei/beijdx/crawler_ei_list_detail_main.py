#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests, random, lxml, json, time, uuid, re
from bs4 import BeautifulSoup
from utils.mysql_list_client import MysqlClient
from kafka import TopicPartition
from utils import kafka_client, file_utils, browser_useragents_utils, redis_client,config

"""
    爬取wos详情信息设计思路：
        1、从kafka topic读取论文列表数据，一次读取3条，采用消费后同步commit方式，确保数据消费安全性
        2、解析从kafka读取的列表数据并进行爬取论文详情信息，组织数据写入mysql库，同步写入爬取进度信息至redis库
            如果爬取失败，程序随机暂停2至5秒后接着进行重试，重试超过五次，放弃此条论文信息并写入redis失败列表，
            继续下一条论文信息爬取
            1、从论文信息中获取论文docindex和docid
            2、根据docindex和docid查询论文详情，解析论文详情完毕后，还需解析参考文献对应的分页信息
            3、所有信息统一入库mysql or redis等等
"""
# 本程序采用灵活可配置方式，暂时采用统一配置文件key，value形式，待爬虫管理系统研发完成，配置方式统一迁入管理库
class SpiderMain(object):

    # 列表爬虫类构造函数：用于初始化程序，从配置文件config.py中获取配置，用于初始化kafka集群，redis，mysql，爬虫参数等
    def __init__(self):
        self.exportvalue = config.exportvalue
        # 初始化mysql客户端
        self.mysqlclient = MysqlClient()
        # kafka客户端初始化以及配置初始化
        self.kafka_client = kafka_client.KafkaClient()
        self.topic = config.kafka_topic
        self.partition = config.kafka_consumer_config.get("partition")
        self.group_id = config.kafka_consumer_config.get("group_id")
        self.consumer = self.kafka_client.get_consumer(self.group_id)
        self.producer = self.kafka_client.get_producer()
        # self.kafka_client = None
        # self.topic = None
        # self.partition = None
        # self.group_id = None
        # self.consumer = None
        # self.producer = None
        # redis客户端初始化以及配置初始化
        self.redis_client = redis_client.RedisClient().get_connection()
        # 消费失败的数据存入redis缓存集合中，最后重试使用
        # 消费失败
        self.consumer_list_fail = config.redis_list_name + "consumer_faile"
        # 消费成功
        self.consumer_list_success = config.redis_list_name + "consumer_success"
        # 消费成功但是没有爬取到数据
        self.consumer_list_success_fail = config.redis_list_name + "consumer_success_fail"


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

    # 获取url，因为url中包含了302重定向的地址以及searchid
    def get_url(self,accession_number):
        headers, cookies = self.base_url()
        if headers is not None:
            count = 0
            while count < 5:
                time.sleep(random.uniform(1, 3000) / 1000)
                session = requests.session()
                root = "https://www.engineeringvillage.com/search/submit.url?usageOrigin=searchform&usageZone=expertsearch&editSearch=&isFullJsonResult=true&angularReq=true&CID=searchSubmit&searchtype=Expert&origin=searchform&category=expertsearch&searchWord1=" + accession_number + "&database=1&yearselect=yearrange&startYear=1884&endYear=2018&updatesNo=1&sort=relevance&autostem=true&searchStartTimestamp=1526625510393&_=" + str(int(round(time.time() * 1000)))
                headers = {
                    'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                           }
                response = session.get(root, headers=headers, cookies = cookies, allow_redirects=False)
                cookies = requests.utils.dict_from_cookiejar(session.cookies)
                print(response.status_code)
                response.close()
                if response.status_code == 302:
                    return response.headers["location"], headers, cookies
                count = count + 1
                continue


    """
        解析论文详情信息,并且入库，包含参考论文信息
    """
    def craw_detail(self, url, headers, cookies, it):
        time.sleep(random.uniform(1, 3000) / 1000)
        searchid = file_utils.url_parse(url, "SEARCHID")
        #print(url)
        session = requests.session()
        response_list = session.get(url, headers=headers, cookies=cookies)
        cookies = requests.utils.dict_from_cookiejar(session.cookies)
        headers = response_list.headers
        #print(response_list.text)
        response_list.close()
        if response_list.status_code == 200 and 'System error happened' not in response_list.text:
            title = ""
            accession_number = ""
            source_title = ""
            language = ""
            document_type = ""
            abstract = ""
            number_of_references = ""
            main_heading = ""
            controlled_terms = ""
            uncontrolled_terms = ""
            classification_code = ""
            doi = ""
            database = ""
            conference_name = ""
            conference_date = ""
            conference_location = ""
            conference_code = ""
            mumerical_data_indexing = ""
            affiliation_no = ""
            author_affiliation = ""
            affiliation_organization = ""
            country = ""
            authors = ""
            affiliation_no = ""
            e_mail = ""
            funding_number = ""
            funding_acronym = ""
            funding_sponsor = ""
            source_title =""
            abbreviated_source_title = ""
            issn = ""
            e_issn = ""
            coden = ""
            isbn_13 = ""
            article_number = ""
            issue = ""
            volume = ""
            part_number = ""
            issue_title = ""
            issue_date = ""
            publication_year = ""
            page_begin = ""
            page_end = ""
            publisher = ""
            referance_no = ""
            referance_title = ""
            referance_authors = ""
            referance_source = ""

            list_json = response_list.json()
            results = list_json["results"]
            docindex = results[0].get("doc").get("hitindex")
            docid = results[0].get("doc").get("docid")

            # abstracthref = results[0]["abstracthref"].replace("\n","").replace(" ","")
            time.sleep(random.uniform(1, 3000) / 1000)
            abstracthref = "https://www.engineeringvillage.com/search/doc/abstract.url?content=true&&pageType=quickSearch&usageZone=resultslist&usageOrigin=searchresults&searchtype=Quick&SEARCHID=" + searchid + "&DOCINDEX=" + str(docindex) + "&ignore_docid=" + docid + "&database=1&format=quickSearchAbstractFormat&tagscope=&displayPagination=yes"
            #session = requests.session()
            # response = session.get(self.basd_url+abstracthref,headers=headers,cookies=cookies)
            headers["Content-Type"] = "application/json"
            # headers["Connection"] = "keep-alive"
            # headers["Referer"] = "https://www.engineeringvillage.com/search/doc/abstract.url?content=true&&pageType=quickSearch&usageZone=resultslist&usageOrigin=searchresults&searchtype=Quick&SEARCHID="+searchid+"&DOCINDEX="+str(docindex)+"&ignore_docid="+docid+"&database=1&format=quickSearchAbstractFormat&tagscope=&displayPagination=yes"

            # abstract_response = session.get(abstracthref, headers=headers, cookies=cookies)
            # print(abstract_response.text)
            # abstract_json = abstract_response.json()
            # title = BeautifulSoup(abstract_json.get("abstractDetail_highlight_terms_map").get("title"),"lxml").text

            # ------------------------------------------------------detailed----------------------------------------------------------
            time.sleep(random.uniform(1, 3000) / 1000)
            detailedhref = "https://www.engineeringvillage.com/search/doc/detailed.url?content=true&SEARCHID=" + searchid + "&DOCINDEX=" + str(docindex) + "&database=1&pageType=expertSearch&searchtype=Expert&dedupResultCount=null&format=expertSearchDetailedFormat&usageOrigin=recordpage&usageZone=abstracttab"
            session = requests.session()
            detailed_response = session.get(detailedhref, headers=headers, cookies=cookies)
            #print(detailed_response.text)
            detailed_response.close()
            if detailed_response.status_code == 200:
                detailed_json = detailed_response.json()
                #print(detailed_json)
                detailed_result = detailed_json.get("result")
                title = BeautifulSoup(detailed_json.get("result").get("title"), "lxml").text.replace("'", "\\'").replace('"', '\\"')
                accession_number = detailed_result.get("accnum")
                author_affiliations = detailed_result.get("affils")
                source_title = detailed_result.get("ril")
                language = detailed_result.get("la")
                document_type = detailed_result.get("doctype")
                abstract = BeautifulSoup(detailed_json.get("abstractDetail_highlight_terms_map").get("abstractRecord"),"lxml").text if detailed_json.get("abstractDetail_highlight_terms_map").get("abstractRecord") is not  None else ''
                number_of_references = detailed_result.get("abstractrecord").get("refcount")
                main_heading = ''
                if detailed_result.get("abstractrecord") is not None:
                    if detailed_result.get("abstractrecord").get("termmap") is not None:
                        if detailed_result.get("abstractrecord").get("termmap").get("MH") is not None:
                            main_heading = detailed_result.get("abstractrecord").get("termmap").get("MH")[0].get("value")
                controlled_terms = BeautifulSoup(detailed_json.get("abstractDetail_highlight_terms_map").get("CVS"),"lxml").text if detailed_json.get("abstractDetail_highlight_terms_map").get("CVS") is not None else ''
                uncontrolled_terms = BeautifulSoup(detailed_json.get("abstractDetail_highlight_terms_map").get("FLS"),"lxml").text if detailed_json.get("abstractDetail_highlight_terms_map").get("FLS") is not  None else ''
                # 具体解析
                classification_code_tmp = detailed_result.get("abstractrecord").get("classificationcodes").get("Classification code")
                if classification_code_tmp is not None and len(classification_code_tmp) >0:
                    for cc in classification_code_tmp:
                        classification_code = classification_code + cc.get("id")+cc.get("title")+" - "
                    classification_code = classification_code.rstrip(' - ')
                doi = detailed_result.get("doi")
                data_base = detailed_result.get("doc").get("dbname")
                conference_name = BeautifulSoup(detailed_result.get("cf"), "lxml").text if detailed_result.get("cf") is not None else ''
                conference_date = detailed_result.get("md") if detailed_result.get("md") is not None else ''
                conference_location = detailed_result.get("ml") if detailed_result.get("ml") is not None else ''
                conference_code = BeautifulSoup(detailed_result.get("cc"), "lxml").text.replace("\n", "").replace("\t", "") if detailed_result.get("cc") is not None else ""
                mumerical_data_indexing = detailed_result.get("ndi") if detailed_result.get("ndi") is not None else ''

                sqls = []
                # ei_thesis_thesis
                tt_cauthors = detailed_result.get("cauthors")
                corresponding_author = ""
                corresponding_author_email = ""
                if tt_cauthors is not None and len(tt_cauthors) > 0:
                    for cauthor in tt_cauthors:
                        corresponding_author = corresponding_author + cauthor.get("name") + ";"
                        corresponding_author_email = corresponding_author_email + ((cauthor.get("email") + ";") if cauthor.get("email") !='' is not None else '')
                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number))
                sql = "REPLACE INTO ei_thesis_thesis(id,title,accession_number,source_title,language,document_type,abstract,number_of_references,main_heading,controlled_terms,uncontrolled_terms,classification_code,doi,data_base,conference_name,conference_date,conference_location,conference_code,mumerical_data_indexing,corresponding_author,corresponding_author_email) " \
                      "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + source_title.replace("'", "\\'").replace('"', '\\"') + "','" + language + "','" + document_type + "','" + abstract.replace("'", "\\'").replace('"', '\\"') + "','" + str(number_of_references) + "','" + main_heading + "','" + controlled_terms.replace("'", "\\'").replace('"', '\\"') + "','" + uncontrolled_terms.replace("'", "\\'").replace('"', '\\"') + "','" + classification_code + "','" + doi + "','" + data_base + "','" + conference_name.replace("'", "\\'").replace('"', '\\"') + "','" + conference_date + "','" + conference_location.replace("'", "\\'").replace('"', '\\"') + "','" + conference_code + "','" + mumerical_data_indexing + "','" + corresponding_author.replace("'", "\\'").replace('"', '\\"') + "','" + corresponding_author_email + "')"
                sqls.append(sql)

                # ei_thesis_affiliation
                if author_affiliations is not None and len(author_affiliations) > 0:
                    for af in author_affiliations:
                        author_affiliation = BeautifulSoup(af.get("name"), "lxml").text if af.get("name") is not None else ''
                        aocs = author_affiliation.split(",")
                        affiliation_organization = ''
                        country = ''
                        if len(aocs) == 5:
                            affiliation_organization = aocs[-3]
                            country = aocs[-1]
                        elif len(aocs) == 4:
                            affiliation_organization = aocs[-3]
                            country = aocs[-1]
                        elif len(aocs) == 3:
                            affiliation_organization = aocs[-2]
                            country = aocs[-1]
                        id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + str(af.get("id"))))
                        sql = 'REPLACE INTO ei_thesis_affiliation(id,title,accession_number,affiliation_no,author_affiliation,affiliation_organization,country)  ' \
                              'VALUES ("' + id + '","' + title + '","' + accession_number + '","' + str(af.get("id")) + '","' + author_affiliation + '","' + affiliation_organization + '","' + country + '")'
                        sqls.append(sql)

                    # ei_thesis_author
                    authors = detailed_result.get("authors")
                    cauthors = detailed_result.get("cauthors")
                    if authors is not None and len(authors) >0:
                        for au in authors:
                            affiliation_no = au.get("id")
                            author = au.get("name")
                            e_mail = au.get("email")
                            corresponding_author = '0'
                            if cauthors is not None and len(cauthors) >0:
                                for cauthor in cauthors:
                                    if author == cauthor.get("name"):
                                        corresponding_author = "1"
                            id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + author))
                            sql = "REPLACE INTO ei_thesis_author(id,title,accession_number,author,affiliation_no,e_mail) " \
                                  "VALUES ('"+id+"','"+title+"','"+accession_number+"','"+author.replace("'", "\\'").replace('"', '\\"')+"','"+str(affiliation_no)+"','"+e_mail+"')"
                            sqls.append(sql)

                    # ei_thesis_funding
                    funding_details = detailed_result.get("abstractrecord").get("fundingDetails")
                    if funding_details is not None and len(funding_details) >0:
                        for fd in funding_details:
                            id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + str(fd.get("fundingId"))))
                            sql = "REPLACE INTO ei_thesis_funding(id,title,accession_number,funding_number,funding_acronym,funding_sponsor) " \
                                  "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + str(fd.get("fundingId")) + "','" + fd.get("fundingAcronym") + "','" + fd.get("fundingAgency").replace("'", "\\'").replace('"', '\\"') + "')"
                            sqls.append(sql)

                    # ei_thesis_publication
                    abbreviated_source_title = detailed_result.get("sourceabbrev")
                    issn = detailed_result.get("citedby").get("issn") if detailed_result.get("citedby").get("issn") is not None else ''
                    e_issn = detailed_result.get("abstractrecord").get("eissn") if detailed_result.get("abstractrecord").get("eissn") is not None else ''
                    if e_issn is not None and e_issn !='':
                        e_issn = e_issn[0:4] + "-" + e_issn[4:len(e_issn)]
                    coden = detailed_result.get("abstractrecord").get("coden") if detailed_result.get("abstractrecord").get("coden") is not None else ''
                    isbn_13 = detailed_result.get("isbn13") if detailed_result.get("isbn13") is not None else ''
                    article_number = detailed_result.get("articlenumber") if detailed_result.get("articlenumber") is not None else ''
                    issue = detailed_result.get("citedby").get("firstissue")
                    volume = detailed_result.get("vo")
                    part_number = detailed_result.get("cfpnum") if detailed_result.get("cfpnum") is not None else ''
                    issue_title = detailed_result.get("mt").replace("::H:",":H::")
                    issue_date = detailed_result.get("sd")
                    publication_year = detailed_result.get("yr")
                    pages = detailed_result.get("pages")
                    page_begin = ""
                    page_end = ""
                    pages_split = pages.split("-")
                    if len(pages_split) == 2:
                        page_begin = pages_split[0]
                        page_end = pages_split[1]
                    publisher = detailed_result.get("pn").replace("::H:",":H::")
                    id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number))
                    sql = "REPLACE INTO ei_thesis_publication(id,title,accession_number,source_title,abbreviated_source_title,issn,e_issn,coden,isbn_13,article_number,issue,volume,part_number,issue_title,issue_date,publication_year,page_begin,page_end,publisher) " \
                          "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + source_title.replace("'", "\\'").replace('"', '\\"') + "','" + abbreviated_source_title.replace("'", "\\'").replace('"', '\\"') + "','" + str(issn) + "','" + str(e_issn) + "','" + str(coden) + "','" + str(isbn_13) + "','" + str(article_number) + "','" + str(issue) + "','" + volume + "','" + str(part_number) + "','" + issue_title.replace("'", "\\'").replace('"', '\\"') + "','" + issue_date + "','" + publication_year + "','" + page_begin + "','" + page_end + "','" + publisher.replace("'", "\\'").replace('"', '\\"') + "')"
                    sqls.append(sql)

                # ------------------------------------------------------Compendex Refs------------------------------------------------------
                # refs1,如果没有参考文献信息，detailed_result.get("abstractrecord").get("refcount")的值会为-1，否则就显示实际论文数
                if number_of_references !=-1:
                    time.sleep(random.uniform(1, 3000) / 1000)
                    refshref = "https://www.engineeringvillage.com/search/doc/refs.url?content=true&refType=compendex&searchtype=Expert&usageOrigin=recordpage&usageZone=detailedtab&pageType=expertSearch&SEARCHID=" + searchid + "&DOCINDEX=" + str(docindex) + "&database=1&docid=" + docid + "&totalResultsCount=67010&displayPagination=yes&dbid=cpx"
                    session = requests.session()
                    refs_response = session.get(refshref, headers=headers, cookies=cookies)
                    #print(refs_response.text)
                    refs_response.close()
                    if refs_response.status_code == 200:
                        refs_json = refs_response.json()
                        #print(refs_json)
                        referenceBean = refs_json.get("referenceBean")
                        title_authors = referenceBean.get("results")
                        sources = referenceBean.get("resultformat_abssourcelines")
                        if title_authors is not None and len(title_authors) >0:
                            for index in range(0,len(title_authors)):
                                referance_no = index +1
                                referance_authors = ""
                                t_authors = title_authors[index].get("authors")
                                if t_authors is not None and len(t_authors) >0:
                                    for tau in t_authors:
                                        referance_authors = referance_authors + tau.get("name") +";"
                                referance_title = title_authors[index].get("title").replace("'", "\\'").replace('"', '\\"')
                                referance_authors = referance_authors.replace("'", "\\'").replace('"', '\\"')
                                referance_source = BeautifulSoup(sources[index], "lxml").text.replace("'", "\\'").replace('"', '\\"').replace('Source:  ', '')
                                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + referance_title))
                                sql = "REPLACE INTO ei_thesis_reference(id,title,accession_number,referance_no,referance_title,referance_authors,referance_source) " \
                                      "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + str(referance_no) + "','" + referance_title + "','" + referance_authors + "','" + referance_source + "')"
                                sqls.append(sql)

                    # resfs2 当refs条数大于25的时候才执行这一步，不然只有一页，没有下一页
                    if number_of_references > 25:
                        time.sleep(random.uniform(1, 3000) / 1000)
                        refshref = "https://www.engineeringvillage.com/search/doc/refs.url?content=true&compendexajax=t&docid=" + docid + "&SEARCHID=" + searchid + "&database=1&DOCINDEX=&currPageNumber=2&searchtype=Expert&pageSize=25"
                        session = requests.session()
                        refs_response = session.get(refshref, headers=headers, cookies=cookies)
                        #print(refs_response.text)
                        refs_response.close()
                        refs_json = refs_response.json()
                        #print(refs_json)
                        referenceBean = refs_json.get("referenceBean")
                        title_authors = referenceBean.get("results")
                        sources = referenceBean.get("resultformat_abssourcelines")
                        if title_authors is not None and len(title_authors) > 0:
                            for index in range(0, len(title_authors)):
                                referance_no = index + 1
                                referance_authors = ""
                                t_authors = title_authors[index].get("authors")
                                if t_authors is not None and len(t_authors) > 0:
                                    for tau in t_authors:
                                        referance_authors = referance_authors + tau.get("name") + ";"
                                referance_title = title_authors[index].get("title").replace("'","\\'").replace('"','\\"')
                                referance_authors = referance_authors.replace("'","\\'").replace('"','\\"')
                                referance_source = BeautifulSoup(sources[index],"lxml").text.replace("'","\\'").replace('"','\\"').replace('Source:  ', '')
                                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + referance_title))
                                sql = "REPLACE INTO ei_thesis_reference(id,title,accession_number,referance_no,referance_title,referance_authors,referance_source) " \
                                      "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + str(referance_no) + "','" + referance_title + "','" + referance_authors + "','" + referance_source + "')"
                                sqls.append(sql)
                #print(sqls)
                self.mysqlclient.insert_thesis_afoprt(sqls)
            else:
                self.redis_client.lpush(self.consumer_list_success_fail, json.dumps(it))
        else:
            self.redis_client.lpush(self.consumer_list_success_fail, json.dumps(it))





    # 根据论文标题和论文作者查询解析论文信息
    def go_to_craw(self, accession_number, it):
        time.sleep(random.uniform(1, 3000) / 1000)
        url, headers, cookies = self.get_url(accession_number)
        if url is not None:
            self.craw_detail(url, headers, cookies, it)

    def craw1(self):
        # 没有参考论文信息 Low-temperature-gradient crystallization for multi-inch high-quality perovskite single crystals for record performance photodetectors
        title_list = [
            "20181304954374","20181705054532","20181705043566","20181304954196","20181705049767",
            "20181204919983","20181705044259","20181204922831","20182105215057","20182105226623",
            "20182105221337","20182105230146","20182105235744","20182105234988","20181204918638",
            "20182105238104","20182105226759","20182105220045","20170503301999","20182105222689",
            "20182105215191","20182105220166","20182105223377","20182105233726","20182205253545"
        ]
        for title in title_list:
            print("--------------------------------------------------------")
            print(title)
            self.go_to_craw(title)
            print("--------------------------------------------------------")
    # 程序入口
    def craw(self):
        print("爬虫开始爬取数据、、、")
        self.consumer.assign([TopicPartition(self.topic, self.partition)])
        #partitions = len(self.producer.partitions_for(self.topic))
        while True:
            consumerRecord = self.consumer.poll(10000, 3)
            values = consumerRecord.values()
            for items in values:
                for item in items:
                    it = json.loads(item.value)
                    docindex = it["docindex"]
                    docid = it["docid"]
                    title = it["title"]
                    accession_number = it["accession_number"]
                    # 查询数据库是否已经存在此条论文信息，存在为True，不存在为False
                    exist = self.mysqlclient.select_thesis_by_accession_number(accession_number)
                    if exist is False:
                        try:
                            print("----------------------------------------------------------------------------------------------------------------------------------------------------")
                            print("topic:%s,partition:%s,offset:%s 消费开始" % (item.topic, item.partition, item.offset))
                            print(it)
                            self.go_to_craw(accession_number,it)
                            self.redis_client.lpush(self.consumer_list_success, json.dumps(it))
                            print("topic:%s,partition:%s,offset:%s 消费成功！" % (item.topic, item.partition, item.offset))
                        except:
                            file_utils.write_to_file1(it, "fail2-3.txt")
                            self.redis_client.lpush(self.consumer_list_fail, json.dumps(it))
                            #self.redis_client.lpush(self.consumer_list_fail, json.dumps(it))
                            #print("topic:%s,partition:%s,offset:%s 消费失败，已写入redis失败列表！" % (item.topic, item.partition, item.offset))
                            # 数据写入kafka,写入kafka已经废弃
                            # future = self.producer.send(topic=self.topic, value=json.dumps(it).encode('utf-8'), key=None, partition=abs(hash(it["thesis_title"])) % partitions, timestamp_ms=None)
                            # recordMetadata = future.get()
                            print("消费失败，已写入redis消费失败列表")
            self.consumer.commit_async()

# 爬虫启动入口
if __name__ == "__main__":
    spiderMain = SpiderMain()
    spiderMain.craw()

