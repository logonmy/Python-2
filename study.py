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




####################################     two day   ############################
a = b = c = 1     #Python允许你同时为多个变量赋值
a, b, c = 1, 2, "runoob"

#Python3 中有六个标准的数据类型：
#
#Number（数字）
#String（字符串）
#List（列表）
#Tuple（元组）
#Sets（集合）
#Dictionary（字典）

a, b, c, d = 20, 5.5, True, 4+3j
print(type(a), type(b), type(c), type(d))
a = 111
isinstance(a, int)    #isinstance() type()

class A:
    pass

class B(A):
    pass

isinstance(A(), A)  # returns True
type(A()) == A      # returns True
isinstance(B(), A)    # returns True
type(B()) == A        # returns False

#type()不会认为子类是一种父类类型。
#isinstance()会认为子类是一种父类类型。

#注意：在 Python2 中是没有布尔型的，它用数字 0 表示 False，用 1 表示 True。
#到 Python3 中，把 True 和 False 定义成关键字了，但它们的值还是 1 和 0，它们可以和数字相加。

var1 = 1
var2 = 10


#del语句的语法是：

del var1,var2

2 // 4 # 除法，得到一个整数

17 % 3 # 取余 

2 ** 5 # 乘方


str = 'Runoob'
 
print (str)          # 输出字符串
print (str[0:-1])    # 输出第一个到倒数第二个的所有字符
print (str[0])       # 输出字符串第一个字符
print (str[2:5])     # 输出从第三个开始到第五个的字符
print (str[2:])      # 输出从第三个开始的后的所有字符
print (str * 2)      # 输出字符串两次
print (str + "TEST") # 连接字符串
print (str + "\nTEST")
print (str + r"\nTEST")

word = 'Python'
print(word[0], word[5])
print(word[-1], word[-6])

与 C 字符串不同的是，Python 字符串不能被改变。向一个索引位置赋值，比如会导致错误。
word[0] = 'm'

#反斜杠(\)可以作为续行符，表示下一行是上一行的延续。也可以使用 """...""" 或者 '''...''' 跨越多行。
#注意，Python 没有单独的字符类型，一个字符就是长度为1的字符串。

#1、反斜杠可以用来转义，使用r可以让反斜杠不发生转义。
#2、字符串可以用+运算符连接在一起，用*运算符重复。
#3、Python中的字符串有两种索引方式，从左往右以0开始，从右往左以-1开始。
#4、Python中的字符串不能改变。


import os
os.getcwd()
os.chdir("D:\\Workspace\\python")

from pandas import Series,DataFrame
import pandas as pd
import numpy as np

import csv
iris=pd.read_csv('iris.csv')
iris = iris.iloc[:,1:6]      #这里的1:6相当于[1,6）前闭后开,iris本身列是0：5
iris.head()
iris.tail()
iris.head(10)

iris.describe()     #in R function summary()
iris.info()         #in R  str()
    
iris['Species'].describe()
iris['Species']     # in R iris$Species
iris['Species'].value_counts() #模拟R的因子分布

#在py中一般应用iloc函数和loc函数，iloc里面需要有数字，而loc里面可以有具体的行列标，举个例子：
# pandas 下的iloc()、loc()
iris.iloc[0:20,0:6]
iris.iloc[:,:]    # iris
iris.iloc[:,5].head()             #iris[,5] #取第五列
iris.loc[:,'Species'].head()      #iris$Species #取Species列    

iris.loc[:,{'Sepal.Length','Species'}].head()   #in R  iris[,c('Sepal.Length','Species')]

iris.iloc[:,1].mean()
iris.iloc[:,1].sum()
iris.iloc[:,1].var()
iris.iloc[:,1].median()
iris.iloc[:,1].max()
iris.iloc[:,1].min()
iris.iloc[:,1].value_counts()
iris.iloc[:,1].quantile(0.95) #pandas 95分位数

iris.iloc[:1,]   #第一行
iris.iloc[:,0]   #第一列
iris.loc[1:10,:]
iris.loc[1:5,]

iris.iloc[0:5,0:2]   #R的用法一直 iris[1:6,1:3]
iris.loc[0:5,['Sepal.Length','Sepal.Width']]
#loc是根据dataframe的具体标签选取列，而iloc是根据标签所在的位置



iris_na=iris.replace(1,np.nan).loc[20:25,:] #replace函数是替换，就是把1替换成nan：
iris_na.dropna(0)    #去除这一行 0 默认 可以不写0
iris_na.dropna(1)    #去除这一列 1
