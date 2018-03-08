# -*- coding: utf-8 -*-
"""
Created on Thu Mar  8 14:26:14 2018
如果需要执行，需要将该py文件 chmod 775 test.py
@author: jelly_q
"""
#####################################     one day   #################################################################
#!/usr/bin/python
# -*- coding: utf-8 -*              # 建议在文件头追加：    # -*- coding: cp936 -*-    或者    # -*- coding: utf-8 -*
print("hello,qw!my love is you...")

# 第一个注释
# 第二个注释

'''
第三注释
第四注释
'''

"""
第五注释
第六注释
"""


str='Runoob'
print(str)                 # 输出字符串
print(str[0:-1])           # 输出第一个到倒数第二个的所有字符
print(str[0])              # 输出字符串第一个字符
print(str[2:5])            # 输出从第三个开始到第五个的字符
print(str[2:])             # 输出从第三个开始的后的所有字符
print(str * 2)             # 输出字符串两次
print(str + '你好')        # 连接字符串

print('------------------------------')

print('hello\nrunoob')      # 使用反斜杠(\)+n转义特殊字符
print(r'hello\nrunoob')     # 在字符串前面添加一个 r，表示原始字符串，不会发生转义

import sys; x = 'runoob'; sys.stdout.write(x + '\n')      #Python可以在同一行中使用多条语句，语句之间使用分号(;)分割，

x="a"
y="b"
# 换行输出
print( x )
print( y )

print('---------')
# 不换行输出
print( x, end=" " )
print( y, end=" " )

import sys
print('================Python import mode==========================');
print ('命令行参数为:')
for i in sys.argv:
    print (i)
print ('\n python 路径为',sys.path)

from sys import argv,path  #  导入特定的成员
 
print('================python from import===================================')
print('path:',path) # 因为已经导入path成员，所以此处引用时不需要加sys.path

