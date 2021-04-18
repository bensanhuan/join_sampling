from loguru import logger
from IndexSample import *


def test_IndexSample():
    sqls = dict()
    with open("./data/sqls.pkl", 'rb') as f:
        sqls = pickle.load(f)
    
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]

    for k, v in sqls.items():
        if k in skipSqls:
            continue
        logger.info("begin indexSample for {}:\n", k)
        estimateCardinality = IndexSample(v, 10000000000, 1000)
        realCardinality = dict()
        with open("./data/realCardinality/" + k + ".realCardinality", 'rb') as f:
            realCardinality = pickle.load(f)
        logger.info("begin compare real / estimate cardinality.............................\n")
        for key in estimateCardinality.keys():
            logger.info("{}, real: {}, estimate: {}, ratio:{} ", key, realCardinality[key], estimateCardinality[key], estimateCardinality[key]/realCardinality[key])
        break