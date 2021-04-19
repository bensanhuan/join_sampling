from QueryGraph import *
import math
import gc
import random
import pickle
import os

samples, estimateCardinality = dict(), dict()
tableBudget, RelationData = dict(), dict()
filter = dict()
maxJoinSize = 6
def TreeIndexSample(sql, budget):
    global samples, estimateCardinality, tableBudget, RelationData, filter
    samples.clear(), estimateCardinality.clear(), tableBudget.clear(), RelationData.clear()
    filter.clear()
    g = QueryGraph(sql)
    root = ExploreOptimizerRoot_1(g)
    tableBudget = distributeBudget_1(g, budget)

    #预处理数据
    Fileds = util.onlyFilterJoinFileds(g)
    for tname in g.tableNames:
        RelationData[tname] = g.data[tname].df[Fileds[tname]]
    #给属性名加上表名前缀防止重复
    for k in RelationData.keys():
        RelationData[k] = util.addPrefix(RelationData[k], k)

    #自顶向下为每张表采样:
    root = ExploreOptimizerRoot_1(g)
    tableBudget = distributeBudget_1(g, budget)
    dfsSampleSingle(root, g)

    #自底向上计算dfs计算所有中间结果
    for tname in g.tableNames:
        pass
        #dfsCaculateJoin(frozenset([tname]), samples[frozenset([tname])], g)
    
    
    #logger.debug("filter:{}\n estimateSingle:{}", filter, estimateCardinality)
    dfs(root, g)

    return estimateCardinality

def dfs(root, g, fa = None):
    global filter
    logger.debug("{}", root)
    froot = frozenset([root])
    old = estimateCardinality[froot]
    if fa != None:
        estimateCardinality[froot] = RelationData[root].shape[0] * filter[root]
    dfsCaculateJoin_2(froot, samples[froot], g, fa)
    estimateCardinality[froot] = old
    for k, v in g.joinCondition[root].items():
        if len(v) > 0 and k != fa:
            dfs(k, g, root)

def dfsSampleSingle(root, g,  fa = None):
    logger.debug("begin sample single for {}....", root)
    global samples, estimateCardinality, tableBudget, f
    #采样
    if fa == None:
        #没有父亲，是根节点，独立采样
        sampleSize = min(tableBudget[root], RelationData[root].shape[0])
        samples[frozenset([root])] = RelationData[root].sample(sampleSize)
    else:
        #根据父亲采样
        f1, f2 = None, None
        for k, v in g.joinCondition[root].items():
            if k == fa:
                f1, f2 = fa + '.' + v[0][1], root + '.' + v[0][0] 
        assert f1 and f2
        
        t2Data = RelationData[root]
        indexList = list()
        #for _, val in samples[frozenset([fa])].iterrows():
        #    indexList = indexList + t2Data[t2Data[f2] == val[f1]].index.values.tolist()
        f1values = list(set( samples[frozenset([fa])][f1].tolist() ))
        indexList = t2Data[t2Data[f2].isin(f1values)].index.values.tolist()
        sampleSize = min(len(indexList), tableBudget[root])
        samplePos = random.sample(indexList, sampleSize)
        samples[frozenset([root])] = t2Data.loc[samplePos]
    #执行单表选择
    tmpDF = util.dropPrefix(samples[frozenset([root])])
    samples[frozenset([root])] = util.addPrefix(performSelect(g, root, tmpDF), root)
    sampleNum = samples[frozenset([root])].shape[0]
    filter[root] = sampleNum / max(1,sampleSize)
    #estimateCardinality[frozenset([root])] = estimateSingle(RelationData[root].shape[0], sampleSize, sampleNum)
    if fa == None:
        estimateCardinality[frozenset([root])] = estimateSingle(RelationData[root].shape[0], sampleSize, sampleNum)
    else:
        x = estimateCardinality[frozenset([fa])] * max(1, len(indexList)) / max(1,samples[frozenset([root])].shape[0])
        estimateCardinality[frozenset([root])] = estimateSingle(len(indexList), sampleSize, sampleNum)


    #logger.debug("sample {}, afterSlect {}", sampleSize, sampleNum)
    #dfs
    for k, v in g.joinCondition[root].items():
        if len(v) > 0 and k != fa:
            dfsSampleSingle(k, g, root)
            
        
def dfsCaculateJoin(s_in, s_in_data, g):
    global samples, estimateCardinality
    if len(s_in) >= maxJoinSize:
        return
    neighbors = g.getNeighbors(s_in)
    for nei in neighbors:
        s_out = set(s_in)
        s_out.add(nei)
        s_out = frozenset(s_out)
        if s_out in estimateCardinality.keys():
            continue
        table, ftable, fnei= None, None, None
        for k, v in g.joinCondition[nei].items():
            if len(v) > 0 and k in s_in:
                table, ftable, fnei = k, k + "." + v[0][1], nei + "." + v[0][0]
        assert table and ftable and fnei
        if ftable.split('.')[1] == 'id':
            s_out_data = s_in_data.join(samples[frozenset([nei])].set_index(fnei), on = ftable, how = 'inner')
        else:
            s_out_data = samples[frozenset([nei])].join(s_in_data.set_index(ftable), on = fnei, how = 'inner')
        estimateCardinality[s_out] = estimateJoin(s_in_data.shape[0], estimateCardinality[s_in], samples[frozenset([nei])].shape[0],\
            estimateCardinality[frozenset([nei])], s_out_data.shape[0])
        logger.debug("estimateCardinality of {}: {}", s_out, round(estimateCardinality[s_out]))
        
        dfsCaculateJoin(s_out, s_out_data, g)        
        del s_out_data
    gc.collect()

def dfsCaculateJoin_2(s_in, s_in_data, g, fa):
    global samples, estimateCardinality
    if len(s_in) >= maxJoinSize:
        return
    neighbors = g.getNeighbors(s_in)
    for nei in neighbors:
        if nei == fa:
            continue
        s_out = set(s_in)
        s_out.add(nei)
        s_out = frozenset(s_out)
        if s_out in estimateCardinality.keys():
            continue
        table, ftable, fnei= None, None, None
        for k, v in g.joinCondition[nei].items():
            if len(v) > 0 and k in s_in:
                table, ftable, fnei = k, k + "." + v[0][1], nei + "." + v[0][0]
        assert table and ftable and fnei
        if ftable.split('.')[1] == 'id':
            s_out_data = s_in_data.join(samples[frozenset([nei])].set_index(fnei), on = ftable, how = 'inner')
        else:
            s_out_data = samples[frozenset([nei])].join(s_in_data.set_index(ftable), on = fnei, how = 'inner')
        estimateCardinality[s_out] = estimateJoin(s_in_data.shape[0], estimateCardinality[s_in], samples[frozenset([nei])].shape[0],\
            estimateCardinality[frozenset([nei])], s_out_data.shape[0])
        logger.debug("estimateCardinality of {}: {}, sampleNum: {}", s_out, round(estimateCardinality[s_out]), s_out_data.shape[0])
        
        dfsCaculateJoin_2(s_out, s_out_data, g, fa)        
        del s_out_data
    gc.collect()


#选择度多的点作为根
def ExploreOptimizerRoot_1(g):
    assert isinstance(g, QueryGraph)
    root, maxd = 0, 0
    for tname1 in g.joinCondition.keys():
        d = 0
        for _, v in g.joinCondition[tname1].items():
            d = d + 1 if len(v) > 0 else d
        if d > maxd:
            root, maxd = tname1, d
    return root        

#只按照表的大小，小表全采，大表均分
def distributeBudget_1(g, budget):
    tableBudget, tableSize = dict(), dict()
    for tname in g.tableNames:
        tableSize[tname] = g.data[tname].df.shape[0]
        tableBudget[tname] = 0
    n = len(g.tableNames)
    remain, changed = budget, True
    while remain > 0 and changed:
        changed = False
        x = math.floor(remain / n)
        if x == 0:
            break
        remain = remain - x * n
        for tname in g.tableNames:
            if tableSize[tname] <= tableBudget[tname]:
                continue
            if tableSize[tname] <= tableBudget[tname] + x:
                n, remain = n - 1, remain + max(0, tableBudget[tname] + x - tableSize[tname])
            tableBudget[tname] = min(tableSize[tname], tableBudget[tname] + x)
            changed = True #防止budget很大时，始终分不完
    return tableBudget
        
def estimateSingle(tableSize, sampleSize, samplesNum):
    #为单表估算基数
    #tableSize: size of origin table
    #sampleSize: sample times
    #samplesNum: Effective sample size
    #kartz backoff处理零概率问题（超级简化版）
    sampleSize, samplesNum = max(1, sampleSize), max(1, samplesNum)
    return tableSize * samplesNum / sampleSize

def estimateJoin(aSamplesNum, aEstimate, bSamplesNum, bEstimate, outSamplesNum):
    #为两表join估算结果基数
    #提供两表分别的样本数，估计值，连接后结果数目
    aSamplesNum, bSamplesNum, outSamplesNum = max(1, aSamplesNum), max(1, bSamplesNum), max(1, outSamplesNum)
    aEstimate, bEstimate = max(1, aEstimate), max(1, bEstimate)
    return outSamplesNum/( (aSamplesNum/aEstimate) * (bSamplesNum/bEstimate) )
    
if __name__ == "__main__":
    #logger.remove()
    logger.add("logs/4_19_treeIndexSample_log", level="INFO")
    with open("data/sqls.pkl", 'rb') as f:
        sqlsDict = pickle.load(f)
    doneFlist = list() #os.listdir("./data/treeEstimateCardinality")
    for i in range(len(doneFlist)):
        doneFlist[i] = doneFlist[i].split('_')[0]
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]
    logger.info("-----------------begin--------------\n")
    for k, sql in sqlsDict.items():
        if k in skipSqls or k in doneFlist:
            continue
        #k, sql = '23a', sqlsDict['23a']
        logger.info("begin caculate for {}", k)
        budget = 100000
        estimateCardinality = TreeIndexSample(sql, budget)
        f = k + "_budget_" + str(budget) + ".treeEstimateCardinality"
        with open("data/treeEstimateCardinality/" + f, 'wb') as f:
            pickle.dump(estimateCardinality, f)
        with open("data/realCardinality/" + k + ".realCardinality", 'rb') as f:
            realCardinality = pickle.load(f)
        ratioList = list()
        for k in realCardinality.keys():
            if k not in estimateCardinality.keys():
                logger.warning("{} not in both", k)
                continue
            ratioList.append(round(math.log(max(1,estimateCardinality[k])/max(realCardinality[k], 1), 10), 2))
        logger.info("ratio:{}\n", ratioList)
        