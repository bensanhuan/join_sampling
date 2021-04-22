from pgdb import connect
import util
from moz_sql_parser import parse
from moz_sql_parser import format
from QueryGraph import *
import copy
from loguru import logger
import pickle
import realCardinality

conn = connect(database = 'job-imdb', host = '127.0.0.1:5432')

def calRealCardinality(sql):
    global conn
    cursor = conn.cursor()
    #初始化，枚举所有中间结果
    g = QueryGraph(sql, True)
    IntermediateSet = set()
    for tname in g.tableNames:
        dfsForSubJoin(frozenset([tname]), g, IntermediateSet)
    
    psql = parse(sql)
    #替换select族为‘count *’
    psql[util.s_select] = {'value': {'count': '*'}}
    realCardinality = dict()
    for inter in IntermediateSet:
        xsql = copy.deepcopy(psql)
        #删除from族多余的
        delList = list()
        for v in xsql[util.s_from]:
            assert isinstance(v, dict)
            if v[util.s_name] not in inter:
                delList.append(v)
        for v in delList:
            xsql[util.s_from].remove(v)
        #删除where族里多余的
        delList = list()
        if util.s_and in xsql[util.s_where].keys():
            for v in xsql[util.s_where][util.s_and]:
                #先判断是不是join条件：
                if util.s_eq in v.keys() and util.IsJoinDes(v[util.s_eq]):
                    t1, t2 = v[util.s_eq][0].split('.')[0], v[util.s_eq][1].split('.')[0]
                    if t1 not in inter or t2 not in inter:
                        delList.append(v)
                elif g._GetTableNameFromSelect(v) not in inter:
                    delList.append(v)
            for v in delList:
                xsql[util.s_where][util.s_and].remove(v)
        else:
            logger.error("unexpected sql: {}", xsql)
        #db查询结果
        #logger.debug("where:{}, len: {}", xsql[util.s_where], len(xsql[util.s_where]))
        if len(xsql[util.s_where][util.s_and]) == 0:
            xsql.pop(util.s_where)
        logger.debug("format xsql for {}: {}", inter, format(xsql))
        cursor.execute(format(xsql))
        realCardinality[inter] = (cursor.fetchone()).count
        logger.debug("result: {}", realCardinality[inter])
    return realCardinality
        
        



def dfsForSubJoin(s_in, g, IntermediateSet):
    assert isinstance(s_in, frozenset)
    if len(s_in) > 6:
        return
    IntermediateSet.add(s_in)
    neighbors = g.getNeighbors(s_in)
    for nei in neighbors:
        s_out = set(s_in)
        s_out.add(nei)
        s_out = frozenset(s_out)
        if s_out in IntermediateSet:
            continue
        dfsForSubJoin(s_out, g, IntermediateSet)


if __name__ == "__main__":
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]
    testSql = "1d"
    with open("./data/sqls.pkl", 'rb') as f:
        sqls = pickle.load(f)
    
    #with open("./data/realCardinality/" + testSql+ ".realCardinality", 'rb') as f:
    #    ref = pickle.load(f)
    # realCardinality.caculateRealCardinality1_1(testSql, sqls[testSql], True)
    # with open("./data/realCardinality/" + testSql+ ".realCardinality", 'rb') as f:
    #    ref = pickle.load(f)
    logger.add("./log/4_21_check_pandasCar_DBCar", level = "INFO")
    #校验pandas和DB估算的误差
    #logger.info("begin check....")
    for kk in sqls.keys():
        if kk in skipSqls:
            continue
        # realCardinality.caculateRealCardinality1_1(k, sqls[k], True)
        # with open("./data/realCardinality/" + testSql+ ".realCardinality", 'rb') as f:
        #     pandasCar = pickle.load(f)
    
        # dbCar = calRealCardinality(sqls[k])
        # for key in pandasCar.keys():
        #     assert key in dbCar.keys()
        #     if pandasCar[key] != dbCar[key]:
        #         logger.warning("{} :key:{}, ref:{}, this: {}", k, key, pandasCar[key], dbCar[key])

        logger.debug("begin cal {}.......", kk)
        DBCar = calRealCardinality(sqls[kk])
        with open("./data/DBrealCardinality/" + kk + ".DBrealCardinality", 'wb') as f:
            pickle.dump(DBCar, f)
        pandasCar = realCardinality.caculateRealCardinality1_1(kk, sqls[kk], True)
            
        for key in DBCar.keys():
            if key not in pandasCar.keys():
                logger.warning("{} not int both", key)
                continue
            if DBCar[key] != pandasCar[key]:
                logger.warning("key: {}, db:{}, pandas:{}", key, DBCar[key], pandasCar[key])

        
        logger.debug("save {}", kk)
    
    
    