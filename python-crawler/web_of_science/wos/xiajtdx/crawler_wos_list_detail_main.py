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
            1、从论文信息中获取论文title和author
            2、根据title和author查询论文列表，从列表中获取论文详情地址
            3、根据论文详情地址获取论文详情，解析论文详情完毕后，还需解析参考文献对应的分页信息
            4、所有信息统一入库mysql or redis等等
"""
# 本程序采用灵活可配置方式，暂时采用统一配置文件key，value形式，待爬虫管理系统研发完成，配置方式统一迁入管理库
class SpiderMain(object):

    # 列表爬虫类构造函数：用于初始化程序，从配置文件config.py中获取配置，用于初始化kafka集群，redis，mysql，爬虫参数等
    def __init__(self):
        self.base_url = 'http://apps.webofknowledge.com'
        # 初始化mysql客户端
        self.mysqlclient = MysqlClient()
        # kafka客户端初始化以及配置初始化
        self.kafka_client = kafka_client.KafkaClient()
        self.topic = config.kafka_topic
        self.partition = config.kafka_consumer_config.get("partition")
        self.group_id = config.kafka_consumer_config.get("group_id")
        self.consumer = self.kafka_client.get_consumer(self.group_id)
        self.producer = self.kafka_client.get_producer()
        # redis客户端初始化以及配置初始化
        self.redis_client = redis_client.RedisClient().get_connection()
        # 消费失败的数据存入redis缓存集合中，最后重试使用
        #消费失败
        self.consumer_list_fail = config.redis_list_name+"consumer_faile"
        #消费成功
        self.consumer_list_success = config.redis_list_name+"consumer_success"
        #消费成功但是没有爬取到数据
        self.consumer_list_success_fail = config.redis_list_name+"consumer_success_fail"
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
        # 所有数据库
        self.product = 'WOS'
        self.general_url = 'WOS_GeneralSearch.do'

        # 中国科学引文数据库 SM
        # self.product = 'CSCD'
        # self.general_url = 'CSCD_GeneralSearch.do'

        # MEDLINE
        # self.product = 'MEDLINE'
        # self.general_url = 'MEDLINE_GeneralSearch.do'

    # 获取sid
    def get_sid(self):
        root = 'http://www.webofknowledge.com/'
        response = requests.get(root, headers={'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS)})
        sid = file_utils.url_parse(response.url, "SID")
        response.close()
        return sid

    # 参考文献列表解析-reference表-解析数据内容，返回为[{}]
    def get_analysis_from_page(self, sid, current_page):
        result_list = []
        headers = {
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
        }
        url = "http://apps.webofknowledge.com/summary.do?product=" + self.product + "&parentProduct=" + self.product + "&search_mode=CitedRefList&parentQid=1&parentDoc=1&qid=2&SID=" + sid + "&colName=WOS&page=" + str(current_page)
        url = "http://apps.webofknowledge.com/summary.do?product=" + self.product + "&parentProduct=" + self.product + "&search_mode=CitedRefList&parentQid=1&parentDoc=1&qid=3&SID=" + sid + "&colName=WOS&page=" + str(current_page)
        time.sleep(random.uniform(1, 3000) / 1000)
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            item_parent = soup.find("div", class_="search-results")
            if item_parent is not None:
                items = item_parent.find_all("div", class_="search-results-item")
                if len(items) > 0:
                    for index in range(0, len(items)):
                        # id = ""
                        # title = ""
                        # accession_number = ""
                        _reference_no = ""
                        _reference_title = ""
                        _reference_source = ""
                        _reference_volume = ""
                        _reference_issue = ""
                        _reference_pages_begin = ""
                        _reference_pages_end = ""
                        _reference_publish_year = ""
                        _reference_authors = ""
                        _reference_cover_date = ""
                        _reference_no = items[index].find("div", class_="search-results-number-align").contents[0].replace(".", "").strip()
                        if items[index].find(class_="reference-title") is not None:
                            _reference_title = items[index].find(class_="reference-title").find("value").text

                        spans = items[index].find_all("span", class_="label")
                        if spans is not None:
                            for span in spans:
                                if span.text.strip() == "作者:" or span.text.strip() == "By:":
                                    _reference_authors = span.next_sibling
                                elif span.text.strip() == "卷:" or span.text.strip() == "Volume:":
                                    _reference_volume = span.next_sibling.find("value").text

                                    # 特殊情况，根据卷来取source
                                    if span.previous_sibling is not None:
                                        if span.previous_sibling.previous_sibling is not None:
                                            if span.previous_sibling.previous_sibling.name == "value":
                                                _reference_source = span.previous_sibling.previous_sibling.text
                                elif span.text.strip() == "页:" or span.text.strip() == "Pages:":
                                    # _reference_pages_begin/_reference_pages_end
                                    pages = span.next_sibling.find("value").text
                                    spl_page = pages.split("-")
                                    _reference_pages_begin = pages
                                    _reference_pages_end = pages
                                    if len(spl_page) == 2:
                                        _reference_pages_begin = spl_page[0]
                                        _reference_pages_end = spl_page[1]

                                    # 特殊情况，根据卷来取source
                                    if span.previous_sibling is not None:
                                        if span.previous_sibling.previous_sibling is not None:
                                            if span.previous_sibling.previous_sibling.name == "value":
                                                _reference_source = span.previous_sibling.previous_sibling.text
                                elif span.text.strip() == "出版年:" or span.text.strip() == "Published:":
                                    # _reference_cover_date/_reference_publish_year
                                    cy = span.next_sibling.find("value").text
                                    _reference_publish_year = cy
                                    _reference_cover_date = ""
                                    spl_page = cy.split(" ")
                                    # 由于发现有些论文的出版年会出现年、年月、年月日的情况
                                    if len(spl_page) == 2:
                                        _reference_publish_year = spl_page[1]
                                        _reference_cover_date = spl_page[0]
                                    elif len(spl_page) == 3:
                                        _reference_publish_year = spl_page[2]
                                        _reference_cover_date = spl_page[0]
                                elif span.text.strip() == "期:" or span.text.strip() == "Issue:":
                                    _reference_issue = span.next_sibling.find("value").text
                                elif span.text.strip() == "丛书:" or span.text.strip() == "Book Series:":
                                    _reference_volume = _reference_volume + "@" + span.next_sibling.find("value").text

                        # 处理_reference_source值情况，最少两种不同的情况需要考虑才能取到值
                        # 目前采用三种方式取值，实在是没法匹配，最后一种在上面的循环中 for span in spans:这里面
                        source_title1 = items[index].find("source_title_" + str(index + 1))
                        if source_title1 is not None:
                            _reference_source = source_title1.find("value").text

                        source_title2 = items[index].find(id="show_journal_overlay_link_" + str(index + 1))
                        if source_title2 is not None:
                            _reference_source = source_title2.find("span").find("value").text

                        result_list.append({"reference_no": _reference_no, "reference_title": _reference_title, "reference_source": _reference_source, "reference_volume": _reference_volume, "reference_issue": _reference_issue, "reference_pages_begin": _reference_pages_begin, "reference_pages_end": _reference_pages_end, "reference_publish_year": _reference_publish_year, "reference_authors": _reference_authors, "reference_cover_date": _reference_cover_date})
            response.close()
        return result_list

    """
        方法描述：根据sid，论文名称，论文作者查询论列表文信息
        参数说明：
            sid:该网站首次访问会在后台生成sid，用来进行身份识别
            value_input1:论文名称，参数名称是根据该网站请求的formdata参数而来，因此命名为value_input1
            value_input2:论文作者，参数名称是根据该网站请求的formdata参数而来，因此命名为value_input2
    """

    def crawl_url(self, sid, value_input1, value_input2):
        root_url = self.base_url + '/' + self.general_url
        self.form_prid_hearders = {
            'Origin': 'https://apps.webofknowledge.com',
            'Referer': 'https://apps.webofknowledge.com/WOS_GeneralSearch_input.do?product=' + self.product + '&search_mode=GeneralSearch&SID=' + sid + '&preferencesSaved=',
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        self.form_prid_data = {
            "fieldCount": "2",
            "action": "search",
            "product": self.product,
            "search_mode": "GeneralSearch",
            "SID": sid,
            "max_field_count": "25",
            "max_field_notice": "注意: 无法添加另一字段。",
            "input_invalid_notice": "检索错误: 请输入检索词。",
            "exp_notice": "检索错误: 专利检索词可在多个家族中找到 (",
            "input_invalid_notice_limits": "<br/>注: 滚动框中显示的字段必须至少与一个其他检索字段相组配。",
            "sa_params": self.product + "||" + sid + "|http://apps.webofknowledge.com|'",
            "formUpdated": "true",
            "value(input1)": value_input1,
            "value(select1)": "TI",
            "value(hidInput1)": "",
            "value(bool_1_2)": "AND",
            "value(input2)": value_input2,
            "value(select2)": "AU",
            "value(hidInput2)": "",
            "limitStatus": "collapsed",
            "ss_lemmatization": "On",
            "ss_spellchecking": "Suggest",
            "SinceLastVisit_UTC": "",
            "SinceLastVisit_DATE": "",
            "period": "Range Selection",
            "range": "ALL",
            "startYear": "1900",
            "endYear": "2018",
            "update_back2search_link_param": "yes",
            "ssStatus": "display:none",
            "ss_showsuggestions": "ON",
            "ss_query_language": "auto",
            "ss_numDefaultGeneralSearchFields": "1",
            "rs_sort_by": "PY.D;LD.D;SO.A;VL.D;PG.A;AU.A"
        }
        s = requests.Session()
        response = s.post(root_url, data=self.form_prid_data, headers=self.form_prid_hearders)
        response.close()
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            search_results = soup.find("div", class_="search-results")
            if search_results is not None:
                search_results_item_list = search_results.find_all("div", class_="search-results-item")
                if search_results_item_list is not None and len(search_results_item_list) > 0:
                    if len(search_results_item_list) == 1:
                        search_results_content = search_results_item_list[0].find("div", class_="search-results-content")
                        if search_results_content is not None:
                            a_href = search_results_content.find("a")["href"]
                            if a_href is not None:
                                return self.base_url + a_href
        return None

    # 参考文献分页处理
    def loop_page_byTotalNum(self, url):
        headers = {
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
        }
        time.sleep(random.uniform(1, 3000) / 1000)
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        total_page = int(soup.find(id="pageCount.top").text)

        sid = file_utils.url_parse(url, "SID")

        # 默认第一页开始
        current_page = 1
        page_size = 30
        result_list = []
        while current_page <= total_page:
            print("本条论文的参考文献第%s页开始爬取，爬取%d条数据" % (current_page, page_size))
            # 循环抓取数据
            page_only_list = self.get_analysis_from_page(sid, current_page)
            if page_only_list is not None:
                result_list = result_list + page_only_list
            print("本条论文的参考文献爬取第%s页数据成功！" % current_page)
            print("--------------------忧伤的分割线----------------------")
            current_page += 1
        response.close()
        return result_list

    """
        解析论文详情信息,并且入库，包含参考论文信息
    """
    def craw_detail(self, url, it):
        headers = {
            'User-Agent': random.choice(browser_useragents_utils.USER_AGENTS),
        }
        time.sleep(random.uniform(1, 3000) / 1000)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "lxml")
            datacontainer = soup.find("div", class_="NEWfullRecordDataContainer")
            # 1、左侧主题内容部分
            if datacontainer is not None:
                l_content = datacontainer.find("div", class_="l-content")
                title = l_content.find("div", class_="title").find("value").text.replace("'", "\\'").replace('"', '\\"').replace('\n', '')

                abstract = ""
                source = ""
                volume = ""
                issue = ""
                page_begin = ""
                page_end = ""
                special_issue = ""
                book_series = ""
                isbn = ""
                doi = ""
                cover_date = ""
                publish_year = ""
                document_type = ""
                author_keywords = ""
                keywords_plus = ""
                # 作者信息里面的通讯作者地址上的通讯作者名称，用于和作者信息做比较，对比成功的reprint字段值设置为1
                auth_names = []
                # 存储作者信息，可能有多条信息，所以采用列表存字典[{},{}]形式
                author_information = []
                # 存储email信息
                emails = []

                # 基金资助致谢(包括机构和授权号),由于有多个数据，所以采用列表存字典[{},{}]形式
                # grant_agency grant_id
                grant_agency_id = []
                funding_text = ""

                conference = ""
                conference_location = ""
                conference_date = ""
                sponsors = ""
                article_number = ""

                publisher = ""
                research_areas = ""
                wos_categories = ""
                language = ""
                accession_number = ""
                pubmed_id = ""
                issn = ""
                eissn = ""
                ids_number = ""
                cited_references_in_woscc = ""
                edition = ""
                authors = []
                fn_researchid_orcid = {}
                block_record_info_item = l_content.find_all("div", class_="block-record-info")
                if len(block_record_info_item) > 0:
                    for item in block_record_info_item:
                        title3_option = item.find("div", class_="title3")
                        # 作者、卷、期、文献等
                        if title3_option == None:
                            if item.find(text=re.compile("作者")) or item.find(text=re.compile("By:")):
                                # 作者信息，由于作者信息dom结构不规范，因此使用获取所有文本节点信息然后进行分割遍历获取
                                ass = item.find("p", class_="FR_field")
                                if ass is not None:
                                    ass_text = ass.get_text()
                                    if ass_text is not None:
                                        ass_texts = ass_text.replace("By:", "").replace("\n", "").split(";")
                                        _sequence_no = 0
                                        for asn in ass_texts:
                                            _sequence_no += 1
                                            _sn_ = re.search("(^.*?\()", asn)
                                            _sn_ = str(_sn_.group(0)).replace("(", "") if _sn_ is not None else ""
                                            _fn_ = re.search("(\(.*?\))", asn)
                                            _fn_ = str(_fn_.group(0)).replace("(", "").replace(")", "") if _fn_ is not None else ""
                                            _an_ = re.search("(\[.*?\])", asn.replace(" ", ""))
                                            _an_ = str(_an_.group(0)).replace("[", "").replace("]", "") if _an_ is not None else ""
                                            _wos_standard_name = _sn_.strip()
                                            _full_name = _fn_
                                            _address_no = ""
                                            if _an_ != "":
                                                _ans_ = _an_.split(",")
                                                if len(_ans_) > 0:
                                                    for _an in _ans_:
                                                        _address_no = _address_no + _an + ";"
                                            authors.append({"wos_standard_name": _wos_standard_name, "full_name": _full_name, "address_no": _address_no, "sequence_no": _sequence_no})
                            elif item.find(text=re.compile("Volume:")) or item.find(text=re.compile("卷:")) or item.find(text=re.compile("DOI")) \
                                    or item.find(text=re.compile("出版年:")) or item.find(text=re.compile("文献类型:")) or item.find(text=re.compile("Document Type:")):
                                source = item.find("p", class_="sourceTitle").find("value").text
                                # 卷、期、页、、、
                                brisv = item.find("div", class_="block-record-info-source-values")
                                if brisv is not None:
                                    bvs = brisv.find_all("p", class_="FR_field")
                                    if bvs is not None:
                                        for bv in bvs:
                                            if bv.find("span", class_="FR_label").text == "卷:" or bv.find("span", class_="FR_label").text == "Volume:":
                                                volume = bv.find("value").text
                                            elif bv.find("span", class_="FR_label").text == "期:" or bv.find("span", class_="FR_label").text == "Issue:":
                                                issue = bv.find("value").text
                                            elif bv.find("span", class_="FR_label").text == "页:" or bv.find("span", class_="FR_label").text == "Pages:":
                                                pages = bv.find("value").text
                                                page_begin = pages.split("-")[0]
                                                page_end = pages.split("-")[1]
                                            elif bv.find("span", class_="FR_label").text == "特刊:" or bv.find("span", class_="FR_label").text == "Special Issue:":
                                                special_issue = bv.find("value").text
                                # DOI、出版年、文献类型
                                dpds = item.find_all("p", class_="FR_field", recursive=False)
                                if dpds is not None:
                                    for dpd in dpds:
                                        if dpd.find("span").text == "文献号:" or dpd.find("span").text == "Article Number:":
                                            article_number = dpd.find("value").text
                                        if dpd.find("span").text == "DOI:":
                                            doi = dpd.find("value").text
                                        if dpd.find("span").text == "丛书:" or dpd.find("span").text == "Book Series:":
                                            book_series = dpd.find("value").text
                                        if dpd.find("span").text == "出版年:" or dpd.find("span").text == "Published:":
                                            dy = dpd.find("value").text.split(" ")
                                            if len(dy) == 1:
                                                cover_date = ""
                                                publish_year = dy[0]
                                            elif len(dy) == 2:
                                                cover_date = dy[0]
                                                publish_year = dy[1]
                                            elif len(dy) == 3:
                                                cover_date = dy[0]
                                                publish_year = dy[2]
                                        if dpd.find("span").text == "文献类型:" or dpd.find("span").text == "Document Type:":
                                            document_type = dpd.contents[2]
                        else:
                            # 摘要：Thesis_Thesis.Abstract
                            if title3_option.text == "摘要" or title3_option.text == "Abstract":
                                abstract = item.find("p", class_="FR_field").text.replace("\n", "")
                            # 关键词
                            elif title3_option.text == "关键词" or title3_option.text == "Keywords":
                                kws = item.find_all("p")
                                if len(kws) > 0:
                                    # 作者关键词
                                    aks = kws[0].find_all("a")
                                    if len(aks) > 0:
                                        for ak in aks:
                                            author_keywords = author_keywords + ak.text + ";"
                                    # keywords plus
                                    if len(kws) > 1:
                                        awps = kws[1].find_all("a")
                                        if len(awps) > 0:
                                            for awp in awps:
                                                keywords_plus = keywords_plus + awp.text + ";"
                            elif title3_option.text == "会议名称" or title3_option.text == "Conference":
                                cfs = item.find_all("p", class_="FR_field")
                                if cfs is not None:
                                    for cf in cfs:
                                        clds = cf.find("span", class_="FR_label")
                                        if clds.text.strip() == "会议:" or clds.text.strip() == "Conference:":
                                            conference = cf.find("value").text
                                        elif clds.text.strip() == "会议地点:" or clds.text.strip() == "Location:":
                                            conference_location = cf.find("value").text
                                        elif clds.text.strip() == "会议日期:" or clds.text.strip() == "Date:":
                                            conference_date = cf.find("value").text
                                        elif clds.text.strip() == "会议赞助商:" or clds.text.strip() == "Sponsor(s):":
                                            sponsors = clds.next_sibling
                            # 作者信息
                            elif title3_option.text == "作者信息" or title3_option.text == "Author Information":
                                ps = item.find_all("p", class_="FR_field")
                                if ps is not None:
                                    for p in ps:
                                        if p.find(text=re.compile("通讯作者地址:")) or p.find(text=re.compile("Reprint Address:")):
                                            auth_names = p.contents[2].replace(" (通讯作者)", "").replace(" (reprint author) ", "").split(";")
                                            rtable = p.next_sibling
                                            trs = rtable.find_all("tr")
                                            if trs is not None:
                                                for tr in trs:
                                                    td1 = tr.find("td", class_="fr_address_row2")
                                                    aocszcs = td1.contents[0]

                                                    _organization = ""
                                                    _country = ""
                                                    _state = ""
                                                    _zip_code = ""
                                                    _city = ""
                                                    _suborganization = ""

                                                    _address_no = ""
                                                    if aocszcs is not None:
                                                        _full_address_list = aocszcs.split(",")
                                                        _organization = ",".join(_full_address_list[0:-1])
                                                        _country = _full_address_list[-1].strip()
                                                        if len(_full_address_list) > 3:
                                                            if bool(re.search(r'\d', _full_address_list[-2])):
                                                                _cz = _full_address_list[-2].strip().split(" ")
                                                                if len(_cz) == 2:
                                                                    _city = _cz[0]
                                                                    _zip_code = _cz[1]
                                                            elif bool(re.search(r'\d', _full_address_list[-3])):
                                                                _state = _full_address_list[-2].strip()
                                                                _cz = _full_address_list[-3].strip().split(" ")
                                                                if len(_cz) == 2:
                                                                    _city = _cz[0]
                                                                    _zip_code = _cz[1]

                                                        _suborganization = _full_address_list[1].strip()

                                                    oen = td1.find("span", recursive=False)
                                                    _organization_enhanced_name = ""
                                                    if oen is not None:
                                                        _preferred_orgs = oen.find_all("preferred_org")
                                                        if len(_preferred_orgs) > 0:
                                                            for _preferred_org in _preferred_orgs:
                                                                if _preferred_org.find("span") is not None:
                                                                    _organization_enhanced_name = _preferred_org.find("span").text + ";"
                                                                else:
                                                                    _organization_enhanced_name = _organization_enhanced_name + _preferred_org.text + ";"
                                                    author_information.append(
                                                        {"full_address": aocszcs, "address_no": _address_no,
                                                         "organization_enhanced_name": _organization_enhanced_name,
                                                         "organization": _organization, "country": _country, "state": _state,
                                                         "zip_code": _zip_code, "city": _city,
                                                         "suborganization": _suborganization})
                                        elif p.find(text=re.compile("^地址:")) or p.find(text=re.compile("^Addresses:")):
                                            rtable = p.next_sibling
                                            trs = rtable.find_all("tr")
                                            if trs is not None:
                                                for tr in trs:
                                                    td1 = tr.find("td", class_="fr_address_row2")
                                                    aocszcs = td1.find("a").text
                                                    # 解析a标签数据
                                                    _address_no_ = re.search("(\[.*?\])", aocszcs)
                                                    _address_no_ = str(_address_no_.group(0)).replace("[", "").replace("]", "").strip() if _address_no_ is not None else ""
                                                    _address_no = _address_no_
                                                    _full_address = re.sub("\[.*?\]", "", aocszcs).strip()

                                                    _organization = ""
                                                    _country = ""
                                                    _state = ""
                                                    _zip_code = ""
                                                    _city = ""
                                                    _suborganization = ""

                                                    if aocszcs is not None:
                                                        _full_address_list = aocszcs.split(",")
                                                        _organization = ",".join(_full_address_list[0:-1])
                                                        _country = _full_address_list[-1].strip()
                                                        if len(_full_address_list) > 3:
                                                            if bool(re.search(r'\d', _full_address_list[-2])):
                                                                _cz = _full_address_list[-2].strip().split(" ")
                                                                if len(_cz) == 2:
                                                                    _city = _cz[0]
                                                                    _zip_code = _cz[1]
                                                            elif bool(re.search(r'\d', _full_address_list[-3])):
                                                                _state = _full_address_list[-2].strip()
                                                                _cz = _full_address_list[-3].strip().split(" ")
                                                                if len(_cz) == 2:
                                                                    _city = _cz[0]
                                                                    _zip_code = _cz[1]

                                                        _suborganization = _full_address_list[1].strip()

                                                    oen = td1.find("span", recursive=False)
                                                    _organization_enhanced_name = ""
                                                    if oen is not None:
                                                        _preferred_orgs = oen.find_all("preferred_org")
                                                        if len(_preferred_orgs) > 0:
                                                            for _preferred_org in _preferred_orgs:
                                                                if _preferred_org.find("span") is not None:
                                                                    _organization_enhanced_name = _preferred_org.find("span").text + ";"
                                                                else:
                                                                    _organization_enhanced_name = _organization_enhanced_name + _preferred_org.text + ";"
                                                    author_information.append({"full_address": _full_address, "address_no": _address_no, "organization_enhanced_name": _organization_enhanced_name, "organization": " ".join(_organization), "country": _country, "state": _state, "zip_code": _zip_code, "city": _city, "suborganization": _suborganization})
                                        elif p.find("span").text == "电子邮件地址:" or p.find("span").text == "E-mail Addresses:":
                                            email_list = p.find_all("a")
                                            if email_list is not None and len(email_list) > 0:
                                                for em_a in email_list:
                                                    emails.append(em_a.text)

                            # 基金资助致谢
                            elif title3_option.text == "基金资助致谢" or title3_option.text == "Funding":
                                # grant_agency
                                # grant_id
                                trs = item.find("table").find_all("tr", class_="fr_data_row")
                                for tr in trs:
                                    tds = tr.find_all("td")
                                    grant_agency = tds[0].text.strip()
                                    # 处理id，有可能有多个ID，因此使用分号隔开
                                    tids = tds[1].find_all("div")
                                    grant_id = ""
                                    if len(tids) > 0:
                                        for tid in tids:
                                            grant_id = grant_id + tid.text.strip() + ";"
                                    grant_agency_id.append({"grant_agency": grant_agency, "grant_id": grant_id})
                                # funding_text
                                funding_text = item.find(id="show_fund_blurb").p.text
                            # 出版商
                            elif title3_option.text == "出版商" or title3_option.text == "Publisher":
                                publisher = item.find("p").find("value").text
                            # 出版商
                            elif title3_option.text == "类别 / 分类" or title3_option.text == "Categories / Classification":
                                rw = item.find_all("p", class_="FR_field")
                                research_areas = rw[0].contents[2]
                                wos_categories = rw[1].contents[2]
                            # 文献信息
                            elif title3_option.text == "文献信息" or title3_option.text == "Document Information":
                                lapie = item.find_all("p", class_="FR_field")
                                if len(lapie) > 0:
                                    for la in lapie:
                                        la_lable = la.find("span", class_="FR_label").text
                                        if la_lable == "语种:" or la_lable == "Language:":
                                            language = la.contents[2]
                                        elif la_lable == "入藏号:" or la_lable == "Accession Number:":
                                            accession_number = la.find("value").text
                                        elif la_lable == "PubMed ID:":
                                            pubmed_id = la.find("value").get_text()
                                        elif la_lable == "ISSN:":
                                            issn = la.find("value").text
                                        elif la_lable == "ISBN:":
                                            isbn = la.contents[2]
                                        elif la_lable == "eISSN:":
                                            eissn = la.find("value").text

                                # language = laie[0].contents[2]
                                # accession_number = laie[1].find("value").text
                                # issn = laie[2].find("value").text
                                # if len(laie) > 3:
                                #     eissn = laie[3].find("value").text
                            # 其他信息
                            elif title3_option.text == "其他信息" or title3_option.text == "Other Information":
                                lr = item.find_all("p", class_="FR_field")
                                ids_number = lr[0].find("value").text
                                if lr[1] is not None:
                                    if lr[1].find("a") is not None:
                                        if lr[1].find("a").find("b") is not None:
                                            cited_references_in_woscc = lr[1].find("a").find("b").text

                # 2、ResearcherID 或 ORCID
                ro_parent = soup.find(id="show_resc_blurb")
                if ro_parent is not None:
                    ro_tr = ro_parent.find_all("tr")
                    if len(ro_tr) == 2:
                        ro_td = ro_tr[1].find_all("td")
                        if len(ro_td) == 3:
                            fn_researchid_orcid = {
                                "full_name": ro_td[0].get_text().replace("\n", "").strip(),
                                "wos_researcherid": ro_td[1].get_text().replace("\n", ""),
                                "orcid_id": ro_td[2].get_text()
                            }
                # 3、右侧侧边栏部分
                r_sidebar = datacontainer.find(id="sidebar-column2")
                ul = r_sidebar.find("ul")
                if ul is not None:
                    us = ul.find_all("span")
                    if len(us) > 0:
                        _edition = ""
                        for index in range(0, len(us)):
                            _edition = _edition + us[index].text.replace("-", "").strip() + ";"
                        edition = _edition

                # thesis_author 表入库数据整理
                sqls = []
                if len(authors) > 0:
                    for index in range(0, len(authors)):
                        # reprint字段数据
                        reprint = "0"
                        for auth_name in auth_names:
                            if authors[index]["wos_standard_name"] == auth_name:
                                reprint = "1"

                        # 处理 email，由于email信息可能比作者数量少
                        if index < len(emails):
                            email = emails[index]
                        else:
                            email = ""

                        # # reprint字段数据
                        wos_researcherid = ""
                        orcid_id = ""
                        if authors[index]["full_name"].upper() == fn_researchid_orcid.get("full_name"):
                            wos_researcherid = fn_researchid_orcid.get("wos_researcherid")
                            orcid_id = fn_researchid_orcid.get("orcid_id")

                        # 两种方式实现数据存在就跟新，不存在就插入，实际上是先delete 再insert的操作
                        # 1、replace INTO thesis_author SET id = '51064a256d15399abcae011bcd9d67e8',title = 'Symmetric Equations for Evaluating Maximum Torsion Stress of Rectangular Beams in Compliant Mechanisms',accession_number = 'WOS:000428335000014',full_name = ' (Chen, Gui-Min)',wos_standard_name = 'Chen, GM',email = 'lhowell@byu.edu',sequence_no = '1',reprint = '0',address_no = '1,2,'
                        # 2、replace INTO thesis_author(id,title,accession_number,full_name,wos_standard_name,email,sequence_no,reprint,address_no) VALUES ('51064a256d15399abcae011bcd9d67e7','Symmetric Equations for Evaluating Maximum Torsion Stress of Rectangular Beams in Compliant Mechanisms','WOS:000428335000014',' (Chen, Gui-Min)','Chen, GM','lhowell@byu.edu','1','0','1,2,')
                        id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + str(authors[index]['sequence_no']) + str(authors[index]['wos_standard_name'])))
                        sql = "REPLACE INTO wos_thesis_author(id,title,accession_number,full_name,wos_standard_name,email,sequence_no,reprint,address_no,wos_researcherid,orcid_id) " \
                              "VALUES ('" + id + "','" + title + "','" + accession_number + "','" + authors[index]['full_name'].replace("'", "\\'").replace('"', '\\"') + "','" + authors[index]['wos_standard_name'].replace("'", "\\'").replace('"', '\\"') + "','" + email + "','" + str(authors[index]['sequence_no']) + "','" + reprint + "','" + authors[index]['address_no'] + "','" + wos_researcherid + "','" + orcid_id + "')"
                        sqls.append(sql)

                # thesis_organization 表入库数据整理
                if len(author_information) > 0:
                    for index in range(0, len(author_information)):
                        id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + str(author_information[index]['full_address']) + str(author_information[index]['address_no'])))
                        sql = 'REPLACE INTO wos_thesis_organization(id,title,accession_number,address_no,full_address,organization,organization_enhanced_name,suborganization,city,state,zip_code,country) ' \
                              'VALUES ("' + id + '","' + title + '","' + accession_number + '","' + str(author_information[index]['address_no']) + '","' + author_information[index]['full_address'] + '","' + author_information[0]['organization'] + '","' + author_information[index]['organization_enhanced_name'] + '","' + author_information[index]['suborganization'] + '","' + author_information[index]['city'] + '","' + author_information[index]['state'] + '","' + author_information[index]['zip_code'] + '","' + author_information[index]['country'] + '") '
                        sqls.append(sql)

                # thesis_funding 表入库数据整理
                if len(grant_agency_id) > 0:
                    for index in range(0, len(grant_agency_id)):
                        id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + str(grant_agency_id[index]['grant_id']) + str(grant_agency_id[index]['grant_agency'])))
                        sql = 'REPLACE INTO wos_thesis_funding(id,title,accession_number,grant_id,grant_agency) ' \
                              'VALUES ("' + id + '","' + title + '","' + accession_number + '","' + str(grant_agency_id[index]['grant_id']) + '","' + str(grant_agency_id[index]['grant_agency']).replace('(', '').replace(')', '').replace("'", "\\'").replace('"', '\\"').strip() + '") '
                        sqls.append(sql)

                # thesis_publication 表入库数据整理
                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number))
                sql = 'REPLACE INTO wos_thesis_publication(id,title,accession_number,source,issn,eissn,publisher,publish_year,volume,issue,page_begin,page_end,ids_number,isbn,book_series,special_issue) ' \
                      'VALUES ("' + id + '","' + title + '","' + accession_number + '","' + source + '","' + issn + '","' + eissn + '","' + publisher + '","' + publish_year + '","' + volume + '","' + issue + '","' + page_begin + '","' + page_end + '","' + ids_number + '","' + isbn + '","' + book_series + '","' + special_issue + '") '
                sqls.append(sql)

                # thesis_thesis 表入库数据整理

                reprint_author = ""
                if len(auth_names) >0:
                    for an in auth_names:
                        reprint_author = reprint_author + an + ";"
                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + author_keywords))
                sql = 'REPLACE INTO wos_thesis_thesis(id,title,accession_number,edition,publish_year,cover_date,source,document_type,abstract,author_keywords,keywords_plus,research_areas,wos_categories,language,cited_references_in_woscc,doi,create_date,funding_text,pubmed_ID,conference,conference_location,conference_date,sponsors,article_number,reprint_author) VALUES ' \
                      '("' + id + '","' + title + '","' + accession_number + '","' + edition + '","' + publish_year + '","' + cover_date + '","' + source + '","' + document_type + '","' + abstract.replace("'", "\\'").replace('"', '\\"') + '","' + author_keywords.replace("'", "\\'").replace('"', '\\"') + '","' + keywords_plus + '","' + research_areas + '","' + wos_categories + '","' + language + '","' + cited_references_in_woscc + '","' + doi + '","' + time.strftime("%Y-%m-%d", time.localtime(int(time.time()))) + '","' + funding_text.replace("'", "\\'").replace('"', '\\"') + '","' + pubmed_id + '","' + conference.replace("'", "\\'").replace('"', '\\"') + '","' + conference_location.replace("'", "\\'").replace('"', '\\"') + '","' + conference_date + '","' + sponsors.replace("'", "\\'").replace('"', '\\"') + '","' + article_number + '","' + reprint_author + '")'
                sqls.append(sql)

                """
                获取引用参考文献信息
               """
                ckwx_model = soup.find_all("div", class_="flex-row flex-justify-start box-div")[1]
                if ckwx_model is not None:
                    if ckwx_model.find("a") is not None:
                        url = ckwx_model.find("a").get("href")
                        result_list = self.loop_page_byTotalNum(self.base_url + "/" + url)

                        # thesis_organization 表入库数据整理
                        if len(result_list) > 0:
                            for index in range(0, len(result_list)):
                                id = str(uuid.uuid3(uuid.NAMESPACE_DNS, title + accession_number + result_list[index]['reference_title'] + result_list[index]['reference_source'] + str(result_list[index]['reference_no'])))
                                sql = 'REPLACE INTO wos_thesis_reference(id,title,accession_number,reference_no,reference_title,reference_source,reference_volume,reference_issue,reference_pages_begin,reference_pages_end,reference_publish_year,reference_authors,reference_cover_date) ' \
                                      'VALUES ("' + id + '","' + title + '","' + accession_number + '","' + str(result_list[index]['reference_no']) + '","' + result_list[index]['reference_title'].replace("'", "\\'").replace('"', '\\"') + '","' + result_list[index]['reference_source'].replace("'", "\\'").replace('"', '\\"') + '","' + result_list[index]['reference_volume'] + '","' + result_list[index]['reference_issue'] + '","' + result_list[index]['reference_pages_begin'] + '","' + result_list[index]['reference_pages_end'] + '","' + result_list[index]['reference_publish_year'] + '","' + result_list[index]['reference_authors'] + '","' + result_list[index]['reference_cover_date'] + '") '
                                sqls.append(sql)
                        self.mysqlclient.insert_thesis_afoprt(sqls)
            else:
                self.redis_client.lpush(self.consumer_list_success_fail, json.dumps(it))
        else:
            self.redis_client.lpush(self.consumer_list_success_fail, json.dumps(it))

        response.close()

    # 根据论文标题和论文作者查询解析论文信息
    def go_to_craw(self, title, author, it):
        time.sleep(random.uniform(1, 3000) / 1000)
        #print("获取sId开始、、、、、、、、、、、")
        sId = self.get_sid()
        #print("sId=", sId)
        #print("获取sId结束、、、、、、、、、、、")
        url = self.crawl_url(sId, title, author)
        if url is not None:
            self.craw_detail(url,it)

    # 程序入口
    def craw(self):
        print("爬虫开始爬取数据、、、")
        self.consumer.assign([TopicPartition(self.topic, self.partition)])
        #self.consumer.subscribe([self.topic])
        partitions = len(self.producer.partitions_for(self.topic))
        while True:
            consumerRecord = self.consumer.poll(10000, 3)
            values = consumerRecord.values()
            for items in values:
                for item in items:
                    it = json.loads(item.value)
                    thesis_title = it["thesis_title"]
                    thesis_authors = it["thesis_authors"]
                    list_authors = thesis_authors.split(";")

                    print("----------------------------------------------------------------------------------------------------------------------------------------------------")
                    # 查询数据库是否已经存在此条论文信息，存在为True，不存在为False
                    exist = self.mysqlclient.select_thesis_by_title(thesis_title.replace("'", "\\'").replace('"', '\\"'))
                    if exist is False:
                        if len(list_authors) > 0:
                            try:
                                print("topic:%s,partition:%s,offset:%s 消费开始" % (item.topic, item.partition, item.offset))
                                print(it)
                                #处理title中的通配符问题，如 *、$、?
                                #tmp_title = it["thesis_title"]
                                #tmp_title.replace("*","").replace("$","").replace("?","")
                                #it["thesis_title"] = tmp_title

                                self.go_to_craw(title=thesis_title.replace("*","").replace("$","").replace("?",""), author=list_authors[0], it=it)
                                self.redis_client.lpush(self.consumer_list_success, json.dumps(it))
                                print("topic:%s,partition:%s,offset:%s 消费成功！" % (item.topic, item.partition, item.offset))
                            except:
                                self.redis_client.lpush(self.consumer_list_fail, json.dumps(it))
                                #self.redis_client.lpush(self.consumer_list_fail, json.dumps(it))
                                #print("topic:%s,partition:%s,offset:%s 消费失败，已写入redis失败列表！" % (item.topic, item.partition, item.offset))
                                # 数据写入kafka,写入kafka已经废弃
                                # future = self.producer.send(topic=self.topic, value=json.dumps(it).encode('utf-8'), key=None, partition=abs(hash(it["thesis_title"])) % partitions, timestamp_ms=None)
                                # recordMetadata = future.get()
                                print("消费失败，已写入redis消费失败列表")
                    else:
                        print("已经存在，无需爬取:%s"%(thesis_title))
            self.consumer.commit()

# 爬虫启动入口
if __name__ == "__main__":
    spiderMain = SpiderMain()
    spiderMain.craw()
