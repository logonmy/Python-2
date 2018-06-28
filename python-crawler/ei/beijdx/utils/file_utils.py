#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
# 获取url中的参数信息
def url_parse(url,key):
    try:
        params = url.split('?')[- 1]
        for param in params.split('&'):
            key_value = param.split("=")
            if key_value[0] == key:
                return key_value[1]
        return None
    except Exception as e:
        print("解析url参数获取失败！",e.message)

# 列表数据写入文件,采用追加的方式
def write_to_file(list_data,filename):
    if len(list_data) >0:
        for index in range(0, len(list_data)):
            with open(filename, "a","utf-8") as f:
                f.write(str(list_data[index]) + "\n")


def write_to_file1(list_data,filename):
    print(list_data)
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(json.dumps(list_data) + "\n")
