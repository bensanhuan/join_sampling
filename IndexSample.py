from QueryGraph import *
import math
import random
import util
import pandas as pd
import gc
from loguru import logger
import pickle
import os
#NOTICE：属性名加了前缀后如果要使用performSelect方法需要删去前缀
#动态规划采样计算中间结果
#为了节约内存，使用dfs

#sampleSize:采样数， samplesNum：样本数量
MaxJoinSize = 6
ReSampleThreshold = 100
sampleSize = 1000
BUDGET = 10000000000000
def IndexSample(sql, budget, sampleSize):
    global BUDGET
    logger.debug("IndexSample for {}, budget:{}, sampleSize:{}", sql, budget, sampleSize)
    BUDGET = budget
    g = QueryGraph(sql)
    samples = dict()
    estimateCardinality = dict()
    #为每个表只留下筛选相关和连接相关属性：
    Fileds = util.onlyFilterJoinFileds(g)
    RelationData = dict()
    for tname in g.tableNames:
        RelationData[tname] = g.data[tname].df[Fileds[tname]]
    #给属性名加上表名前缀防止重复
    for k in RelationData.keys():
        RelationData[k] = util.addPrefix(RelationData[k], k)
    #给每个表采样
    for tname in g.tableNames:
        tableSize = RelationData[tname].shape[0]
        n = min(sampleSize, RelationData[tname].shape[0])
        fs = frozenset([tname])
        samples[fs] = RelationData[tname].sample(n)
        #执行单表选择(需要移除属性前缀，和重新添加前缀)
        samples[fs] = util.addPrefix(performSelect(g, tname, util.dropPrefix(samples[fs])), tname)
        samples[fs] = samples[fs].reset_index(drop = True)
        BUDGET -= n
        estimateCardinality[fs] = estimateSingle(tableSize, n, samples[fs].shape[0])
        logger.debug("estimateCardinality of {}: {}", tname, estimateCardinality[fs])
    #计算中间结果
    IntermediateSizeDict = dict()
    for tname in g.tableNames:
        IntermediateSizeDict[frozenset([tname])] = estimateCardinality[frozenset([tname])]
    for k in g.tableNames:
        dfs(frozenset([k]), samples[frozenset([k])], g, RelationData, sampleSize, estimateCardinality, IntermediateSizeDict)
    return estimateCardinality
    
def dfs(s_in, s_in_data, g, RelationData, sampleSize, estimateCardinality, IntermediateSizeDict):
    assert isinstance(estimateCardinality, dict) and isinstance(IntermediateSizeDict, dict)
    global BUDGET

    if len(s_in) >= MaxJoinSize:
        return
    neighbors = g.getNeighbors(s_in)
    for nei in neighbors:
        s_out = set(s_in)
        s_out.add(nei)
        s_out = frozenset(s_out)
        if s_out in IntermediateSizeDict.keys() and IntermediateSizeDict[s_out] >= ReSampleThreshold :
            continue
        #计算s_out：
        #找到和nei连接的表
        for r in s_in:
            if len(g.joinCondition[r][nei]) <= 0:
                continue
            #采样neiData并filter
            neiData = RelationData[nei]
            f1, f2 = r + '.' + g.joinCondition[r][nei][0][0], nei + '.' + g.joinCondition[r][nei][0][1]
            logger.debug("join {} with {} on {}, {}", s_in, nei, f1, f2)
            if s_in_data.shape[0] > 0:
                indexList = dict()
                #获取nei上能匹配的数据索引
                totalMatch = 0
                for index, val in s_in_data.iterrows():
                    indexList[index] = neiData[neiData[f2] == val[f1]].index.values.tolist()
                    totalMatch += len(indexList[index])
                #确认采样的索引并排序
                samplePos = random.sample(range(totalMatch), min(totalMatch, sampleSize))
                samplePos.sort()
                #计算s_out_data    
                s_out_data = list()
                index, l, r= 0, 0, len(indexList[0])
                joinData = list()
                posList , y= list(),list()
                for x in samplePos:
                    while x - l >= r:
                        l = l + len(indexList[index])
                        index = index + 1
                        r = len(indexList[index])
                    posList.append(indexList[index][x -l])
                    y.append(index)
                    #joinData.append(neiData.iloc[indexList[index][x -l]].tolist() + [index])
                joinData = neiData.loc[posList]
                joinData[nei + '.' + 'index'] = y    
                joinData = util.dropPrefix(joinData)
                joinData = performSelect(g, nei, joinData)
                joinData = joinData.set_index('index')
                joinData = util.addPrefix(joinData, nei)
            else:
                totalMatch = 0
                newColumns = neiData.columns.tolist()
                newColumns.append('index')
                joinData = (pd.DataFrame([], columns = newColumns)).set_index('index')

            s_out_data = s_in_data.join(joinData, how = 'inner').reset_index(drop = True)
            IntermediateSizeDict[s_out] = s_out_data.shape[0]
            estimateCardinality[s_out] = estimateJoin(s_in_data.shape[0], estimateCardinality[s_in], totalMatch, min(totalMatch, sampleSize), joinData.shape[0])
            logger.debug("estimateCardinality of {}: {}", s_out, estimateCardinality[s_out])
            BUDGET = BUDGET - s_in_data.shape[0] - min(totalMatch, sampleSize)
            del joinData, neiData
            dfs(s_out, s_out_data, g, RelationData, sampleSize, estimateCardinality, IntermediateSizeDict)
            del s_out_data
            break
    gc.collect()



def estimateSingle(tableSize, sampleSize, samplesNum):
    #为单表估算基数
    #tableSize: size of origin table
    #sampleSize: sample times
    #samplesNum: Effective sample size
    #kartz backoff处理零概率问题（超级简化版）
    sampleSize, samplesNum = max(1, sampleSize), max(1, samplesNum)
    return tableSize * samplesNum / sampleSize

def estimateJoin(inSamplesNum, InEstimate, totalMatch, sampleSize, samplesNum):
    #为两表join估算结果基数
    
    inSamplesNum, totalMatch, sampleSize, samplesNum = max(inSamplesNum, 1), max(totalMatch, 1), max(sampleSize, 1), max(samplesNum, 1)
    return InEstimate * totalMatch / inSamplesNum * samplesNum / sampleSize

if __name__ == "__main__":
    files = os.listdir("./data/estimateCardinality")
    logger.remove()
    logger.add(sink = "./logs/4_15_IndexSample_log", level="INFO")
    logger.info("\n\n-------------------------begin-----------------------\n")
    sqls = dict()
    with open("./data/sqls.pkl", 'rb') as f:
        sqls = pickle.load(f)
    
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]
    strtemp = "maxJoinSize_" + str(MaxJoinSize) + "_sampleSize_" + str(sampleSize) + "_budget_" + str(BUDGET) + "_reSampleThreshold_" + str(ReSampleThreshold)
    filePath = "data/results/IndexSample/" +strtemp+".ratio"
    i = 0
    for k, v in sqls.items():
        if k in skipSqls:
            continue
        i= i + 1
        filePathOfes = "data/estimateCardinality/"+k+"_" + strtemp+".estimateCardinality"
        if k+"_" + strtemp+".estimateCardinality" in files:
            continue
        logger.info("begin indexSample for {}:", k)
        estimateCardinality = IndexSample(v, 10000000000, 1000)
        with open(filePathOfes, 'wb') as f:
            pickle.dump(estimateCardinality, f)
        realCardinality = dict()
        with open("./data/realCardinality/" + k + ".realCardinality", 'rb') as f:
            realCardinality = pickle.load(f)
        #logger.info("begin compare real / estimate cardinality.............................\n")
        ratioList = list()
        for key in estimateCardinality.keys():
            #logger.info("{}, real: {}, estimate: {}, ratio:{} ", key, realCardinality[key], estimateCardinality[key], estimateCardinality[key]/realCardinality[key])
            if realCardinality[key] == 0:
                ratioList.append(1)
            else:
                ratioList.append(round(estimateCardinality[key]/realCardinality[key], 2))
        logger.info("ratios: {}\n", ratioList)
    with open(filePath, 'wb') as f:
        pickle.dump(ratioList, f)
        