from loguru import logger
from TreeIndexSample import *
import pickle

def test_TreeIndexSample():

    with open("data/sqls.pkl", 'rb') as f:
        sqlsDict = pickle.load(f)
    
    for k, sql in sqlsDict.items():
        logger.info("begin caculate for {}\n", k)
        estimateCardinality = TreeIndexSample(sql, 10000)
        logger.info("result: {}", estimateCardinality)
        break