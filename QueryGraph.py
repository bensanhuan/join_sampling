# -*- coding: utf-8 -*-
from moz_sql_parser import parse
import util
from loguru import logger
import data_process
import pandas as pd
#FIXME: fixme usage sample
#是否采用预采样后的pickel
useSampledPkl = False
#QueryGraph类：内部加载原表数据，解析sql语句生成graph。对外部提供join关系结构，原数据，对一张表的子集执行单表选择的能力。
class QueryGraph:
    def __init__(self, sql, justGenJoinCondition = False):
        #输入sql语句，自动初始化
        pSql = parse(sql)
        self.tableMapping = util.GetTableMapping(pSql)
        tableNames = []
        for k, _ in self.tableMapping.items():
            tableNames.append(k)
        logger.debug("tableNames: {}", tableNames)
        tableNames = list(set(tableNames))
        self.tableNames = tableNames
        
        self.joinCondition = dict()
        for tname in tableNames:
            self.joinCondition[tname] = dict()
            for tname2 in tableNames:
                self.joinCondition[tname][tname2] = []
        
        #处理joinCondition
        #1.可能存在and，如果and，join条件必不可能嵌套
        joinDesList = []
        if util.s_and in pSql[util.s_where].keys():
            for d in pSql[util.s_where][util.s_and]:
                if util.s_eq in d.keys():
                    if util.IsJoinDes(d[util.s_eq]):
                        joinDesList.append(d[util.s_eq]) 
        else:
            if util.IsJoinDes(pSql[util.s_where][util.s_eq]):
                joinDesList.append(pSql[util.s_where][util.s_eq])
        for joindes in joinDesList:
            self._addEdge(joindes)
        #优化joinCondition，eg: a.id = b.aid & a.id = c.aid & b.aid = c.aid，只需要保留任意两个等式
        #对每一个顶点，按照连接属性归类，对于id属性类中（强依赖sql语句的特点），如果一对顶点存在额外的连接，删除
        for tname in tableNames:
            #遍历所有连接, filedConnect = dict[filedName]list[(tablename, filedname)]
            fieldConnect = dict()
            for otname in tableNames:
                for edge in self.joinCondition[tname][otname]:
                    if edge[0] not in fieldConnect:
                        fieldConnect[edge[0]] = list()
                    fieldConnect[edge[0]].append((otname, edge[1]))
            #检查连接是否有额外连接，并清除
            if 'id' not in fieldConnect.keys():
                continue
            v = fieldConnect['id']
            sz = len(v)
            for i in range(0, sz):
                for j in range(0, sz):
                    if i == j:
                        continue
                    x, y = v[i], v[j]
                    if (x[1], y[1]) in self.joinCondition[x[0]][y[0]]:
                        self.joinCondition[x[0]][y[0]].remove((x[1], y[1]))
                    if (y[1], x[1]) in self.joinCondition[y[0]][x[0]]:
                        self.joinCondition[y[0]][x[0]].remove((y[1], x[1]))


        if justGenJoinCondition == True:
            return

        #读取数据
        self.data = dict()
        for name in tableNames:
            self.data[name] = Relation(self.tableMapping[name])
        #筛选出单表选择条件 dict[表名]list[单表选择条件]
        #前提条件，sql语句中被括号括起来的where语句都是针对一个表的
        self.selectDes = dict()
        whereDict = pSql[util.s_where]
        for key, value in whereDict.items():
            if util.isAndOr(key):
                for condition in value:
                    if isinstance(condition, dict) and util.s_eq in condition.keys() and util.IsJoinDes(condition[util.s_eq]):
                        #是两表join条件
                        continue
                    tname=self._GetTableNameFromSelect(condition)
                    if tname not in self.selectDes.keys():
                        self.selectDes[tname] = []
                    self.selectDes[tname].append(condition)
            else:
                if key == util.s_eq and util.IsJoinDes(value):
                    continue
                else:
                    tname = self._GetTableNameFromSelect({key:value})
        #整理select属性集合：
        self.selectFileds = dict()
        for tname in self.tableNames:
            self.selectFileds[tname] = list()
        for k, v in self.selectDes.items():
            for desc in v:
                self.selectFileds[self._GetTableNameFromSelect(desc)] = self.selectFileds[self._GetTableNameFromSelect(desc)] + self._getSelectFileds(desc)
        for tname in self.tableNames:
            self.selectFileds[tname] = list(set(self.selectFileds[tname]))

    def _addEdge(self, joindes):
        t1, t2 = joindes[0].split('.')[0],joindes[1].split('.')[0]
        assert t1 in self.tableNames and t2 in self.tableNames
        f1, f2 = joindes[0].split('.')[1], joindes[1].split('.')[1]
        self.joinCondition[t1][t2].append((f1, f2))
        self.joinCondition[t2][t1].append((f2, f1))
                 
    def _GetTableNameFromSelect(self, condition):
        assert len(condition) > 0
        if isinstance(condition, list):
            return self._GetTableNameFromSelect(condition[0])
        
        for key, value in condition.items():
            if util.isAndOr(key):
                return self._GetTableNameFromSelect(value)
            elif isinstance(value, list):#可能是not null这种情况
                return value[0].split('.')[0]
            else:
                return value.split('.')[0]

    def getNeighbors(self, tnames):
        assert isinstance(tnames, frozenset)
        neighbors = list()
        for tname in tnames:
            assert tname in self.joinCondition.keys()
            for k, v in self.joinCondition[tname].items():
                if len(v) > 0 and k not in tnames and k not in neighbors:
                    neighbors.append(k)
        return neighbors

    def _getSelectFileds(self, desc):
        assert isinstance(desc, dict)
        results = list()
        logger.debug("_getSelectFileds of {}", desc)
        for k, v in desc.items():
            if k == util.s_and or k == util.s_or:
                for subDes in v:
                    results = results + self._getSelectFileds(subDes)
            else:
                field = ''
                if isinstance(v, list):
                    field = v[0].split('.')[1]
                else:
                    field = v.split('.')[1]
                results.append(field)
        #去重
        results = list(set(results))
        return results
class Relation:
    def __init__(self, relationName):
        self.relationName = relationName
        self.df = data_process.load_pickle(relationName, useSampledPkl)    
        # self.estimateSize = self.df.shape[0]
        # if useSampledPkl: 
        #     if relationName in data_process.preSamplePercentages.keys():
        #         self.estimateSize = self.estimateSize / data_process.preSamplePercentages[relationName]

#输入querygraph、tablename，dataframe of table

def performSelect(G, tablename, df):
    assert tablename in G.data.keys()
    beforeSize = df.shape[0]
    if tablename not in G.selectDes.keys():
        return df
    for desc in G.selectDes[tablename]:
        df = _performSelect(df, desc)
    return df

def _performSelect(df, desc):
    assert isinstance(desc, dict)
    #logger.debug("desc: {}", desc)
    for key, value in desc.items():
        if key == util.s_and:
            #递归结果做交集
            tempdfs = [_performSelect(df, subDes) for subDes in value]
            retdf = tempdfs[0]
            for i in range(1, len(tempdfs)):
                retdf = pd.merge(retdf, tempdfs[i], how = 'inner')
            return retdf
        elif key == util.s_or:
            tempdfs = [_performSelect(df, subDes) for subDes in value]
            retdf = tempdfs[0]
            for i in range(1, len(tempdfs)):
                retdf = pd.merge(retdf, tempdfs[i], how = 'outer')
                
            return retdf
        else:
            field = ''
            if isinstance(value, list):
                field = value[0].split('.')[1]
            else:
                field = value.split('.')[1]
            if key == util.s_between:
                l, r = 0,0
                if isinstance(value[1], dict):
                    l, r = min(value[1][util.s_literal], value[2][util.s_literal]), max(value[1][util.s_literal], value[2][util.s_literal])
                else:
                    l, r = min(value[1], value[2]), max(value[1], value[2])
                return df[ (df[field] < r) & (df[field] > l)]
            elif key == util.s_like:
                pattern = value[1][util.s_literal].replace("%", ".*")
                return df[df[field].str.match(pattern) & df[field].notna()]
            elif key == util.s_not_like:
                pattern = value[1][util.s_literal].replace("%", ".*")
                pattern = '^((?!' + pattern + ').)*$'
                return df[df[field].str.match(pattern) & df[field].notna()]
            elif key == util.s_is_null:
                return df[df[field].isna()]
            elif key == util.s_is_not_null:
                return df[df[field].notna()]
            elif key == util.s_eq:
                if isinstance(value[1], dict):
                    #logger.debug("value:{}", value)
                    return df[df[field] == value[1][util.s_literal]]
                return df[df[field] == value[1]]
            elif key == util.s_neq:
                if isinstance(value[1], dict):
                    return df[df[field] != value[1][util.s_literal]]
                return df[df[field] != value[1]]
            elif key == util.s_gt:
                if isinstance(value[1], dict):
                    return df[df[field] > value[1][util.s_literal]]
                return df[df[field] > value[1]]
            elif key == util.s_lt:
                if isinstance(value[1], dict):
                    return df[df[field] < value[1][util.s_literal]]
                return df[df[field] < value[1]]
            elif key == util.s_in:
                if isinstance(value[1], dict):
                    inlist = value[1][util.s_literal]
                    if isinstance(inlist, list) == False:#有的sql语句是IN('hhh')只有一个
                        inlist = [inlist]
                    return df[df[field].isin(inlist)]
                return df[df[field].isin(value[1])]
            elif key == util.s_gte:
                if isinstance(value[1], dict):
                    return df[df[field] >= value[1][util.s_literal]]
                return df[df[field] >= value[1]]
            elif key == util.s_lte:
                if isinstance(value[1], dict):
                    return df[df[field] <= value[1][util.s_literal]]
                return df[df[field] <= value[1]]
            else:
                logger.error("Unkown sql parse slice: {}, {}", key, value)


       