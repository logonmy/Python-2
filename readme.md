# my study py practice
## use spyder ide 

**py 书写是以对其为标准，去除掉了{}的范式
下表是是0开始的，选择的区间是左闭右开 [)**

关于实例中第一行代码#!/usr/bin/python3 的理解：
然后修改脚本权限，使其有执行权限，命令如下：`$ chmod +x hello.py`
分成两种情况：

（1）如果调用python脚本时，使用:
```
$ python script.py 

#!/usr/bin/python 被忽略，等同于注释。

#!/usr/bin/env python3 这种用法先在 env（环境变量）设置里查找 python 的安装路径，再调用对应路径下的解释器程序完成操作。
```

（2）如果调用python脚本时，使用:

./script.py 
```
#!/usr/bin/python 指定解释器的路径。

>>> helloworld
```
再解释一下第一行代码#!/usr/bin/python3

这句话仅仅在linux或unix系统下有作用，在windows下无论在代码里加什么都无法直接运行一个文件名后缀为.py的脚本，因为在windows下文件名对文件的打开方式起了决定性作用。

如果代码有中文，最好开头需要添加这句：

### 多行语句
Python 通常是一行写完一条语句，但如果语句很长，我们可以使用反斜杠(\)来实现多行语句，例如：
```
total = item_one + \
        item_two + \
        item_three
```
在 [], {}, 或 () 中的多行语句，不需要使用反斜杠(\)，例如：
```
total = ['item_one', 'item_two', 'item_three',
        'item_four', 'item_five']
```
同一行显示多条语句
```
#!/usr/bin/python3

import sys; x = 'runoob'; sys.stdout.write(x + '\n')

```
注意：
1、Python可以同时为多个变量赋值，如a, b = 1, 2。
2、一个变量可以通过赋值指向不同类型的对象。
3、数值的除法（/）总是返回一个浮点数，要获取整数使用//操作符。
4、在混合计算时，Python会把整型转换成为浮点数。


----------


### 标准数据类型
**Python3 中有六个标准的数据类型：**

- Number（数字）    Python3 支持 int、float、bool、complex（复数）。
- String（字符串）
- List（列表）
- Tuple（元组）
- Sets（集合）
- Dictionary（字典）

### Number
Python3 支持 int、float、bool、complex（复数）。

在Python 3里，只有一种整数类型 int，表示为长整型，没有 python2 中的 Long。

像大多数语言一样，数值类型的赋值和计算都是很直观的。

内置的 type() 函数可以用来查询变量所指的对象类型。

```
counter = 100          # 整型变量
miles   = 1000.0       # 浮点型变量
name    = "runoob"     # 字符串

print (counter)
print (miles)
print (name)

>>>5 + 4  # 加法
9
>>> 4.3 - 2 # 减法
2.3
>>> 3 * 7  # 乘法
21
>>> 2 / 4  # 除法，得到一个浮点数
0.5
>>> 2 // 4 # 除法，得到一个整数
0
>>> 17 % 3 # 取余 
2
>>> 2 ** 5 # 乘方
32
>>>a = 111
>>> isinstance(a, int)
True
# 删除py内存数据
del var
del var_a, var_b

```
区别就是:
type()不会认为子类是一种父类类型。
isinstance()会认为子类是一种父类类型。
注意：在 Python2 中是没有布尔型的，它用数字 0 表示 False，用 1 表示 True。到 Python3 中，把 True 和 False 定义成关键字了，但它们的值还是 1 和 0，它们可以和数字相加。

### String（字符串）
Python中的字符串用单引号(')或双引号(")括起来，同时使用反斜杠(\)转义特殊字符。
字符串的截取的语法格式如下：
索引值以 0 为开始值，-1 为从末尾的开始位置。
加号 (+) 是字符串的连接符， 星号 (*) 表示复制当前字符串，紧跟的数字为复制的次数。实例如下：

```
>>>word = 'Python'
>>> print(word[0], word[5])
P n
>>> print(word[-1], word[-6])
n P
```

**与 C 字符串不同的是，Python 字符串不能被改变。向一个索引位置赋值，比如word[0] = 'm'会导致错误。**
注意：

- 1、反斜杠可以用来转义，使用r可以让反斜杠不发生转义。
- 2、字符串可以用+运算符连接在一起，用*运算符重复。
- 3、Python中的字符串有两种索引方式，从左往右以0开始，从右往左以-1开始。
- 4、Python中的字符串不能改变。

### List（列表）
List（列表） 是 Python 中使用最频繁的数据类型。

列表可以完成大多数集合类的数据结构实现。列表中元素的类型可以不相同，它支持数字，字符串甚至可以包含列表（所谓嵌套）。

列表是写在方括号([])之间、用逗号分隔开的元素列表。

和字符串一样，列表同样可以被索引和截取，列表被截取后返回一个包含所需元素的新列表。

列表截取的语法格式如下：

```
#!/usr/bin/python3
 
list = [ 'abcd', 786 , 2.23, 'runoob', 70.2 ]
tinylist = [123, 'runoob']
 
print (list)            # 输出完整列表
print (list[0])         # 输出列表第一个元素
print (list[1:3])       # 从第二个开始输出到第三个元素
print (list[2:])        # 输出从第三个元素开始的所有元素
print (tinylist * 2)    # 输出两次列表
print (list + tinylist) # 连接列表
```
### Tuple（元组）
元组（tuple）与列表类似，不同之处在于元组的元素不能修改。元组写在小括号(())里，元素之间用逗号隔开。

元组中的元素类型也可以不相同：
```
#!/usr/bin/python3
 
tuple = ( 'abcd', 786 , 2.23, 'runoob', 70.2  )
tinytuple = (123, 'runoob')
 
print (tuple)             # 输出完整元组
print (tuple[0])          # 输出元组的第一个元素
print (tuple[1:3])        # 输出从第二个元素开始到第三个元素
print (tuple[2:])         # 输出从第三个元素开始的所有元素
print (tinytuple * 2)     # 输出两次元组
print (tuple + tinytuple) # 连接元组

>>>tup = (1, 2, 3, 4, 5, 6)
>>> print(tup[0])
1
>>> print(tup[1:5])
(2, 3, 4, 5)
>>> tup[0] = 11  # 修改元组元素的操作是非法的
```

### Set（集合）
集合（set）是一个无序不重复元素的序列。
基本功能是进行成员关系测试和删除重复元素。
可以使用大括号 { } 或者 set() 函数创建集合，注意：创建一个空集合必须用 set() 而不是 { }，因为 { } 是用来创建一个空字典。
创建格式：
```
parame = {value01,value02,...}
或者
set(value)

#!/usr/bin/python3
 
student = {'Tom', 'Jim', 'Mary', 'Tom', 'Jack', 'Rose'}
 
print(student)   # 输出集合，重复的元素被自动去掉
 
# 成员测试
if('Rose' in student) :
    print('Rose 在集合中')
else :
    print('Rose 不在集合中')
 
 
# set可以进行集合运算
a = set('abracadabra')
b = set('alacazam')
 
print(a)
 
print(a - b)     # a和b的差集
 
print(a | b)     # a和b的并集
 
print(a & b)     # a和b的交集
 
print(a ^ b)     # a和b中不同时存在的元素

```

### Dictionary（字典）
字典（dictionary）是Python中另一个非常有用的内置数据类型。

列表是有序的对象结合，字典是无序的对象集合。两者之间的区别在于：字典当中的元素是通过键来存取的，而不是通过偏移存取。

字典是一种映射类型，字典用"{ }"标识，它是一个无序的键(key) : 值(value)对集合。

键(key)必须使用不可变类型。

在同一个字典中，键(key)必须是唯一的。
```
#!/usr/bin/python3
 
dict = {}
dict['one'] = "1 - 菜鸟教程"
dict[2]     = "2 - 菜鸟工具"
 
tinydict = {'name': 'runoob','code':1, 'site': 'www.runoob.com'}
 
 
print (dict['one'])       # 输出键为 'one' 的值
print (dict[2])           # 输出键为 2 的值
print (tinydict)          # 输出完整的字典
print (tinydict.keys())   # 输出所有键
print (tinydict.values()) # 输出所有值
```
另外，字典类型也有一些内置的函数，例如clear()、keys()、values()等。

注意：

1、字典是一种映射类型，它的元素是键值对。
2、字典的关键字必须为不可变类型，且不能重复。
3、创建空字典使用 { }。

## [Python运算符](http://www.runoob.com/python3/python3-basic-operators.html "运算符")
### Python数据类型转换
|函数	|描述|
|-----|-----|
|int(x [,base])  |    将x转换为一个整数
|float(x)     | 		将x转换到一个浮点数
|complex(real [,imag])		|创建一个复数
|str(x)	|	将对象 x 转换为字符串
|repr(x)	|	将对象 x 转换为表达式字符串
|eval(str)	|	用来计算在字符串中的有效Python表达式,并返回一个对象
|tuple(s)	|	将序列 s 转换为一个元组
|list(s)	|	将序列 s 转换为一个列表
|set(s)		|转换为可变集合
|dict(d)	|	创建一个字典。d 必须是一个序列 (key,value)元组。
|frozenset(s)|		转换为不可变集合
|chr(x)	|	将一个整数转换为一个字符
|ord(x)	|	将一个字符转换为它的整数值
|hex(x)	|	将一个整数转换为一个十六进制字符串
|oct(x)	|	将一个整数转换为一个八进制字符串

### Python算术运算符
- 算术运算符
- 比较（关系）运算符
- 赋值运算符
- 逻辑运算符
- 位运算符
- 成员运算符
- 身份运算符
- 运算符优先级

### Python赋值运算符

|运算符	|描述	|实例   |
|----|-----|-------|
|=|	简单的赋值运算符	|c = a + b 将 a + b 的运算结果赋值为 c|
|+=	|加法赋值运算符	|c += a 等效于 c = c + a
|-=	|减法赋值运算符	|c -= a 等效于 c = c - a
|*=	|乘法赋值运算符	|c *= a 等效于 c = c * a
|/=	|除法赋值运算符	|c /= a 等效于 c = c / a
|%=	|取模赋值运算符	|c %= a 等效于 c = c % a
|**=	|幂赋值运算符	|c **= a 等效于 c = c ** a
|//=	|取整除赋值运算符	|c //= a 等效于 c = c // a

### Python逻辑运算符

|运算符|	逻辑表达式|	描述	实例|
|----|-----|-------|
|and|	x and y	|布尔"与" - 如果 x 为 False，x and y 返回 False，否则它返回 y 的计算值。	(a and b) 返回 20。|
|or	|x or y	|布尔"或" - 如果 x 是 True，它返回 x 的值，否则它返回 y 的计算值。	(a or b) 返回 10。|
|not|	not x	|布尔"非" - 如果 x 为 True，返回 False 。如果 x 为 False，它返回 True。	not(a and b) 返回 False|

### Python成员运算符

|运算符|	描述	|实例|
|----|-----|-------|
|in|	如果在指定的序列中找到值返回 True，否则返回 False。|	x 在 y 序列中 , 如果 x 在 y 序列中返回 True。|
|not in	|如果在指定的序列中没有找到值返回 True，否则返回 False。|	x 不在 y 序列中 , 如果 x 不在 y 序列中返回 True。|



----------

**iris data example operation**
和流行的Python库（如NumPy（线性代数），SciPy（信号和图像处理）或matplotlib（交互式2D / 3D绘图））支持的数值计算环境
```
# -*- coding: utf-8 -*-

import os
os.getcwd()       #获取当前工作空间
os.chdir("D:\\Workspace\\python")     #设置目标工作空间

# 常用的数据处理的包：
from pandas import Series,DataFrame
import pandas as pd
import numpy as np

import csv
iris=pd.read_csv('iris.csv')   #读取本地csv文件
iris = iris.iloc[:,1:6]      #这里的1:6相当于[1,6）前闭后开,iris本身列是0：5
iris.iloc[:,：]              #相当于iris 本身
iris.head()
iris.tail()
iris.head(10)

iris.iloc[:,1].mean()
iris.iloc[:,1].sum()
iris.iloc[:,1].var()
iris.iloc[:,1].median()
iris.iloc[:,1].max()
iris.iloc[:,1].min()
iris.iloc[:,1].value_counts()
iris.iloc[:,1].quantile(0.95) #pandas 95分位数

iris_na=iris.replace(1,np.nan).loc[20:25,:] #replace函数是替换，就是把1替换成nan：
iris_na.dropna(0)    #去除这一行 0 默认 可以不写0
iris_na.dropna(1)    #去除这一列 1
```
pandas的iloc和loc以及**icol**使用（列切片及行切片）

http://www.runoob.com/python3/python3-data-type.html
### Python里的OS模块常用函数说明：
- os.getcwd()函数得到当前工作目录，即当前python脚本工作的目录路径。
- os.getenv()获取一个环境变量，如果没有返回none
- os.putenv(key, value)设置一个环境变量值
- os.listdir(path)返回指定目录下的所有文件和目录名。
- os.remove(path)函数用来删除一个文件。
- os.system(command)函数用来运行shell命令。
- os.linesep字符串给出当前平台使用的行终止符。例如，windows使用'\r\n'，linux使用'\n'而mac使用'\r'。
- os.path.split(p)函数返回一个路径的目录名和文件名。
- os.path.isfile()和os.path.isdir()函数分别检验给出的路径是一个文件还是目录。
- os.path.existe()函数用来检验给出的路径是否真地存在
- os.curdir:返回当前目录（'.')
- os.chdir(dirname):改变工作目录到dirname
- os.path.getsize(name):获得文件大小，如果name是目录返回0l
- os.path.abspath(name):获得绝对路径
- os.path.normpath(path):规范path字符串形式
- os.path.splitext():分离文件名与扩展名
- os.path.join(path,name):连接目录与文件名或目录
- os.path.basename(path):返回文件名
- os.path.dirname(path):返回文件路径

#### Pandas 提供了一些选择的方法，这些选择的方法可以把数据切片，也可以把数据切块。下面我们简单介绍一下：

- 查看一列的一些基本统计信息：`data.columnname.describe()`
- 选择一列：`data['columnname']`
- 选择一列的前几行数据：`data['columnsname'][:n]`
- 选择多列：`data[['column1','column2']]`
- Where 条件过滤：`data[data['columnname'] > condition]`

data.country= data.country.fillna('')    #将NA替换成空
data.duration = data.duration.fillna(data.duration.mean())     #将NA替换成均值

删除任何包含 NA 值的行是很容的： 	data.dropna()
删除一整行的值都为 NA：         	data.dropna(how='all')
行数据中至少要有 5 个非空值		data.drop(thresh=5)
删除列名    						data.dropna(subset=['title_year'])    如果是多个列，可以使用列名的 list 作为参数。
默认是axis=0 行 axis=1 列
删除一正列为 NA 的列：data.drop(axis=1, how='all')
删除任何包含空值的列：data.drop(axis=1. how='any')

data = pd.read_csv('../data/moive_metadata.csv', dtype={'duration': int})  指定列的数据类型 duration为int
data = pd.read_csv('./data/moive_metadata.csv', dtype={'title_year':str})	指定列的数据类型 title_year为str
data['movie_title'].str.upper()     改为大写	
data['movie_title'].str.strip()		去掉空格

#### 数据合并
Pandas： append()		#行合并	 R  dplyr::bind_cols()  cbind
```
result = df1.append(df2);print(result)
result = result.append(df3);print(result)
```
Pandas： cancat()		#列合并	 R 	dplyr::bind_rows()	rbind
` result = pd.concat([df1, df2, df3]);print(result)`
Pandas： merge			#联合 R merge


#### 提取单列的两种等价方式：
```
iris.shape    # R  dim()
df.dtypes		# R  class() typeof()



mydata.model   #在R语言中应该写mydata$model
mydata["model"]  #在R语言中应该写mydata[,"model"]或者mydata["model"]

mydata[["model","manufacturer"]]     #多列提取
mydata[::2]   #默认隔几个单位取一次值
iris[["Sepal.Length","Sepal.Width"]][1:10]

mydata.loc[3]        #按索引提取单行的数值
mydata.loc[0:5]      #按索引提取区域行数值
mydata.loc[1:10,["model","manufacturer"]] #行列同时索引

mydata.iloc[[0,2]]  等价于mydata.iloc[[0,2],:]
mydata.iloc[1:]     等价于mydata.iloc[1:,:]
mydata.iloc[1,[0,1]]
mydata.iloc[:3,:2]          
mydata.iloc[[0,2,5],[4,5]] 

iris.ix[:10,:-2]       #python示意list形式传入,而R是vector传入
iris.ix[:10,['Sepal.Length','Petal.Width']]
iris.ix[:10,'Sepal.Length':'Species']
iris.ix[:10,[0,-1]]

dt.query('sl >5 & pw >2')  #在py中列名最好不要带. 这样的特殊字符，否则会报错，以下以索引的方式不会报错

#where
iris[(iris['Sepal.Length']>4.9) & (iris['Petal.Width'] < 2)] 		#同时满足
iris[(iris['Sepal.Length']>4.9) | (iris['Petal.Width'] < 0.2)] 		#满足其一
iris[(iris['Sepal.Length']>4.9) | (iris['Petal.Width'] < 0.2)][['Species']]   #满足其一，指定列



```
#### panda

```
############################   panda  example  ########################################
from pandas import Series, DataFrame  
import pandas as pd 
arr=[1,2,3,4]

series_1 = Series(arr)
series_2=Series([1,2,3,4]) 
series_3=Series([1,2,'3',4,'a'])

series_4 =Series([1,2,3])
series_4.index=['a','b','c'] #创建索引

temp =Series([5])
type(temp)
series_4.append(temp)       #增    Series的add()方法是加法计算不是增加Series元素用的。
series_4.add(temp)          #对应索引位置的相加
series_4.drop('a')   # 删
series_4['a']=4         #改
series_4['a']       #查
```






























