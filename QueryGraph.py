from moz_sql_parser import parse
import util
import dataLoder
from loguru import logger
import pandas as pd
# TODO: 单例测试本部分代码
#TODO: 添加图优化方法
#FIXME: fixme usage sample
class QueryGraph:
    def __init__(self, sql):
        #输入sql语句，自动初始化
        pSql = parse(sql)
        self.tableMapping = util.GetTableMapping(pSql)
        tableNames = []
        for _, v in self.tableMapping:
            tableNames.append(v)
        tableNames = list(set(tableNames))
        
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

        #读取数据
        self.data = dict()
        for name in tableNames:
            self.data[name] = Relation(name)
        #筛选出单表选择条件 dict[表名]list[单表选择条件]
        #前提条件，sql语句中被括号括起来的where语句都是针对一个表的
        selectDes = dict()
        whereDict = pSql[util.s_where]
        for key, value in whereDict:
            if util.isAndOr(key):
                for condition in util.s_and:
                    if isinstance(condition, dict) and util.s_eq in condition.keys() and util.IsJoinDes(condition[util.s_eq]):
                        #是两表join条件
                        continue
                    tname=self.GetTableNameFromSelect(condition)
                    if tname not in selectDes.keys():
                        selectDes[tname] = []
                    selectDes[tname].append(condition)
            else:
                if key == util.s_eq and util.IsJoinDes(value):
                    continue
                else:
                    tname = self.GetTableNameFromSelect({key:value})
        #为每个表的数据执行单表查询语句并更新单表select后的基数估计
        for tname, descList in selectDes:
            for desc in descList:
                beforeSize = self.data[tname].shape[0]
                self.data[tname] = self._performSelect(tname, desc)
                self.data[tname].estimateSize *= self.data[tname].shape[0] / beforeSize
        

    def _addEdge(self, joindes):
        t1, t2 = joindes[0].split('.')[0],joindes[1].split('.')[0]
        assert t1 in self.tableMapping.keys() and t2 in self.tableMapping.keys()
        tname1, tname2 = self.tableMapping[t1], self.tableMapping[t2]
        f1, f2 = joindes[0].split('.')[1], joindes[1].split('.')[1]
        self.joinCondition[tname1][tname2].append((f1, f2))
        self.joinCondition[tname2][tname1].append((f2, f1))

    #从parse解析出来的dict对dataframe做选择
    #递归函数
    def _performSelect(self, tablename, desc):
        assert tablename in self.data.keys()
        isinstance(desc, dict)
        for key, value in desc:
            if key == util.s_and:
                #递归结果做交集
                tempdfs = [self._performSelect(tablename, subDes) for subDes in value]
                df = tempdfs[0]
                for i in range(1, len(tempdfs)):
                    df = pd.merge(df, tempdfs[i], how = 'inner')
                return df
            elif key == util.s_or:
                tempdfs = [self._performSelect(tablename, subDes) for subDes in value]
                df = tempdfs[0]
                for i in range(1, len(tempdfs)):
                    df = pd.merge(df, tempdfs[i], how = 'outer')
                    
                return df
            else:
                field = value[0].split('.')[1]
                df = self.data[tablename].df
                if key == util.s_between:
                    l, r = 0,0
                    if isinstance(value[1], dict):
                        l, r = min(value[1][util.s_literal], value[2][util.s_literal]), max(value[1][util.s_literal], value[2][util.s_literal])
                    else:
                        l, r = min(value[1], value[2]), max(value[1], value[2])
                    return df[ (df[field] < r) & (df[field] > l)]
                elif key == util.s_like:
                #todo: continue code
                    pattern = value[1][util.s_literal].replace("%", ".*")
                    return df[df[field].str.match(pattern)]
                elif key == util.s_not_like:
                    pattern = value[1][util.s_literal].replace("%", ".*")
                    pattern = '^((?!' + pattern + ').)*$'
                    return df[df[field].str.match(pattern)]
                elif key == util.s_is_null:
                    return df[df[field].isna()]
                elif key == util.s_is_not_null:
                    return df[df[field].notna()]
                elif key == util.s_eq:
                    return df[df[field] == value[1]]
                elif key == util.s_neq:
                    return df[df[field] != value[1]]
                elif key == util.s_gt:
                    return df[df[field] > value[1]]
                elif key == util.s_lt:
                    return df[df[field] < value[1]]
                elif key == util.s_in:
                    return df[df[field].isin(value[1])]
                else:
                    logger.error("Unkown sql parse slice: {}, {}", key, value)
                    
    
    def GetTableNameFromSelect(self, condition):
        for key, value in condition:
            if util.isAndOr(key):
                return self.GetTableNameFromSelect(value)
            else:
                return self.tableMapping[value[0].split('.')[0]]

class Relation:
    def __init__(self, relationName):
        self.relationName = relationName
        self.df = dataLoder.load_pickle(relationName)    
        self.estimateSize = self.df.shape[0] 
        if relationName in util.preSamplePercentages.keys():
            self.estimateSize = self.estimateSize / util.preSamplePercentages[relationName]

    
        
        