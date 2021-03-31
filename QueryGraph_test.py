from QueryGraph import *
from loguru import logger
import matplotlib.pyplot as plt
import pickle

##Usage：run "pytest" in terminal
##if only test one method: pytest -v filePath::className::methodName

# pdb调试usage速查：
# 进入: python3 -m pdb filename.py
# 添加断点: b (filename::)lineNumber 临时添加断点: tbreak lineNumber 清楚断点: cl lineNumber
# 继续执行: c
# 打印变量值: p expression
# 执行下一行: 进入函数体: s; 不进入函数体: n
# 打印变量类型: whatis expression
# 打印堆栈信息: w
# 退出: q



def test_joinConditionGen():
    sqlPath="data/join-order-benchmark/13d.sql"
    logger.add("test_log")
    with open(sqlPath, 'r') as f:
            sql = f.read().replace("\n", " ")
            logger.debug("{}",parse(sql))
            logger.debug("sql: {}", sql)
            logger.info("begin draw for {}", sql)
            sql = sql.replace(';','')
            qg = QueryGraph(sql, True)            

def test_singleTableSelectPerform():
    sqls = list()
    with open("./data/sqls.pkl", 'rb') as f:
        sqls = pickle.load(f)
    g = QueryGraph(sqls['6f'])
    return
    for k, sql in sqls.items():
        logger.debug("test for sql: {}\n", k)
        queryg = QueryGraph(sql)
        
        

if __name__ == "__main__":
    test_joinConditionGen()