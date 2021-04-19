import re
import glob
import pandas as pd
from loguru import logger
import matplotlib.pyplot as plt
#用于sql解析的字符串常量
s_select = 'select'
s_from = 'from'
s_where = 'where'
s_value = 'value'
s_name = 'name'
s_literal = 'literal'
s_is_null = 'missing'
s_is_not_null = 'exists'
s_between = 'between'
s_like = 'like'
s_greater = 'gt'
s_lt = 'lt'
s_gt = 'gt'
s_gte = 'gte'
s_lte = 'lte'
s_eq = 'eq'
s_neq = 'neq'
s_neq = 'neq'
s_or = 'or'
s_and = 'and'
s_like = 'like'
s_not_like = 'not_like'
s_in = 'in'


#给一个parse结果，返回表名映射 
def GetTableMapping(p):
    assert(isinstance(p, dict)) and s_from in p.keys() and isinstance(p[s_from], list)
    tableMapping = dict()
    for v in p[s_from]:
        if isinstance(v, dict):
            #有重命名
            tableMapping[v[s_name]] = v[s_value]
        else:
            assert isinstance(v, str)
            tableMapping[v] = v

    return tableMapping

#判断parse结果中‘eq'对应的list[‘ ’ , ‘ ’]是不是连接语句
def IsJoinDes(x):
    if isinstance(x, list) and len(x) == 2 and isinstance(x[0], str) and isinstance(x[1], str) \
        and re.match(".*\..*", x[0]) and re.match(".*\..*", x[1]) and x[0].split('.')[0] != x[1].split('.')[0]:
        return True
    else:
        return False
#判断字符串是否是‘and' 'or'
def isAndOr(x):
    assert isinstance(x, str)
    return x == s_and or x == s_or

def dropPrefix(df):
    filedNames = df.columns.str.split('.').tolist()
    renameDict = dict()
    for v in filedNames:
        renameDict[v[0] + '.' + v[1]] = v[1]
    df = df.rename(columns = renameDict)
    return df

def onlyFilterJoinFileds(g):
    fileds = dict()
    for tname1 in g.tableNames:
        fileds[tname1] = list()
        for tname2 in g.tableNames:
            for val in g.joinCondition[tname1][tname2]:
                fileds[tname1].append(val[0])
    for tname in g.tableNames:
        fileds[tname] = fileds[tname] + g.selectFileds[tname]
    for tname in g.tableNames:
        fileds[tname] = list(set(fileds[tname]))
    return fileds


def addPrefix(df, pre):
    return df.add_prefix(pre + '.')
        

if __name__ == "__main__":
    pass        
    
    


            


    
