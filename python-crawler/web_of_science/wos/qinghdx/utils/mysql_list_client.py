#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
from utils import config

class MysqlClient(object):
    def __init__(self):
        pass

    def _get_connection(self):
        conn = MySQLdb.connect(host=config.mysql_config.get("host"),
                               port=config.mysql_config.get("port"),
                               user=config.mysql_config.get("user"),
                               passwd=config.mysql_config.get("passwd"),
                               db=config.mysql_config.get("db"),
                               charset=config.mysql_config.get("charset"))
        return conn

    # 根据title查询数据是否存在，在wos_thesis_thesis表查询
    def select_thesis_by_title(self,title):
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            sql = "select * from wos_thesis_thesis WHERE title = '" + title+"' limit 1"
            print("查看本条论文是否已经爬取-sql:%s" % sql)
            cur.execute(sql)
            rs = cur.rowcount
            cur.close()
            conn.commit()
            if rs ==1:
                return True
            return False
        except MySQLdb.Error as e:
            error = 'MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1])
            print("fail-sql:%s" % sql)
            print(error)
        finally:
            self.close_conn(conn, cur)

    # wos六张表数据更新方法
    def insert_thesis_afoprt(self, sqls):
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            for sql in sqls:
                #print(sql)
                cur.execute(sql)
            conn.commit()
        except MySQLdb.Error as e:
            error = 'MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1])
            print("fail-sql:%s"%sql)
            print(error)
            # 写入mysql库失败的sql进行记录
            with open("crawler_wos_list_detail_main.log", "a") as f:
                f.write(str(sql) + ";\n")
        finally:
            self.close_conn(conn, cur)

    def close_conn(self, conn, cur):
        if conn:
            conn.close()
        if cur:
            cur.close()

