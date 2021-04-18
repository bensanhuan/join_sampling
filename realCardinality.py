from QueryGraph import *
import queue
import copy
import pickle
import gc
import os
import time

#TODO: check whether 20a.sql caculate right

#NOTICE：目前只计算大小为6的
#计算一个sql语句中间结果的真实基数
#先对单表数据执行单表选择条件，然后留下需要连接的属性列
MaxJoinSize = 6
MAXN = 100

files = os.listdir("./data/realCardinality")
logger.info("already has {}\n", files)

def caculateRealCardinality(prefix ,sql):
    #检查是否已经计算过：
    if prefix+'.realCardinality' in files:
        logger.info("already caculated\n")
        return

    #内存hold不住的查询先跳过：
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]

    if prefix in skipSqls:
        logger.info("skip {}\n", prefix)
        return
    
    g = QueryGraph(sql)
    relationData = dict()
    for k in g.data.keys():
        relationData[k] = g.data[k].df
    for k in relationData.keys():
        relationData[k] = performSelect(g, k, relationData[k])
    
    #整理需要连接的属性：
    joinField = dict()
    for tname in g.tableNames:
        joinField[tname] = list()
    for tname1 in g.tableNames:
        for tname2 in g.tableNames:
            for x in g.joinCondition[tname1][tname2]:
                if x[0] not in joinField[tname1]:
                    joinField[tname1].append(x[0])         
    #过滤数据
    for k in relationData.keys():
        relationData[k] = relationData[k][joinField[k]]
        logger.debug("caculateRealCardinality relationdata[{}] columns:{}\n", k, relationData[k].columns.values.tolist())

    #修改数据属性列名，为了防止连接后重名，加上表名前缀
    for k in relationData.keys():
        relationData[k]=relationData[k].add_prefix(k + '.')
        logger.debug("caculateRealCardinality relationdata[{}] after rename columns:{}\n", k, relationData[k].columns.values.tolist())

    
    #枚举中间结果并计算
    #由于图上无环，所以每次加入一个新表连接，肯定只有一个join条件
    datas = dict()#使用表名的字符串set作为key
    realCardinality = dict()
    IntermediateList = list() 
    upupSet = set()
    q = queue.Queue()
    for k in relationData.keys():
        datas[frozenset([k])] = relationData[k]
        realCardinality[frozenset([k])] = relationData[k].shape[0]
        q.put(frozenset([k]))
    logger.debug("relationData keys:{}\n", relationData.keys())
    del relationData
    del g.data
    gc.collect()
    while not q.empty():
        s_in = q.get()
        IntermediateList.append(s_in)
        if len(s_in) >= MaxJoinSize:
            continue
        neighbors = g.getNeighbors(s_in)
        for nei in neighbors:
            s_out = set(s_in)
            s_out.add(nei)
            s_out = frozenset(s_out)
            if s_out in realCardinality.keys():#已经计算过了
                continue
            #找到和nei连接的表
            for r in s_in:
                if len(g.joinCondition[r][nei]) <= 0:
                    continue
                #找到连接，做join，选择xxx_id的列
                fields = g.joinCondition[r][nei][0]
                logger.debug("join {} with {} on {}", s_in, nei, fields)
                if fields[0] == 'id':
                    datas[s_out] = datas[s_in].join(datas[frozenset([nei])].set_index(nei + '.' + fields[1]), on = (r + '.' + fields[0]), how = 'inner')
                else:                
                    datas[s_out] = datas[frozenset([nei])].join(datas[s_in].set_index(r + '.' + fields[0]), on=(nei + '.' + fields[1]), how = 'inner')
                logger.debug("  size: {}\n", datas[s_out].shape[0])
                q.put(s_out)
                realCardinality[s_out] = datas[s_out].shape[0]
                break
            if len(s_out) == MaxJoinSize:
                del datas[s_out]
                gc.collect()
        if len(s_in) > 1:
            del datas[s_in]
            gc.collect()        
        

    for k in IntermediateList:
        logger.debug("caculateRealCardinality realCardinality of {}: {}\n", k, realCardinality[k])
    
    with open('./data/realCardinality/'+prefix+'.realCardinality', 'wb') as f:
        pickle.dump(realCardinality, f)
    
    
    
#优化内存占用
def caculateRealCardinality1_1(prefix ,sql):
    #检查是否已经计算过：
    if prefix+'.realCardinality' in files:
        logger.info("already caculated\n")
        return

    #内存hold不住的查询先跳过：
    skipSqls = ["17a", "17b", "17c", "17d", "17e", "17f", "16a", "16b", "16c", "16d"]
    if prefix in skipSqls:
        logger.info("skip {}\n", prefix)
        return
    
    g = QueryGraph(sql)
    relationData = dict()
    for k in g.data.keys():
        relationData[k] = g.data[k].df
    for k in relationData.keys():
        relationData[k] = performSelect(g, k, relationData[k])
    
    #整理需要连接的属性：
    joinField = dict()
    for tname in g.tableNames:
        joinField[tname] = list()
    for tname1 in g.tableNames:
        for tname2 in g.tableNames:
            for x in g.joinCondition[tname1][tname2]:
                if x[0] not in joinField[tname1]:
                    joinField[tname1].append(x[0])         
    #过滤数据
    for k in relationData.keys():
        relationData[k] = relationData[k][joinField[k]]
        logger.debug("caculateRealCardinality relationdata[{}] columns:{}\n", k, relationData[k].columns.values.tolist())
    del g.data
    gc.collect()
    #修改数据属性列名，为了防止连接后重名，加上表名前缀
    for k in relationData.keys():
        relationData[k]=relationData[k].add_prefix(k + '.')
        logger.debug("caculateRealCardinality relationdata[{}] after rename columns:{}\n", k, relationData[k].columns.values.tolist())

    
    #枚举中间结果并计算
    #由于图上无环，所以每次加入一个新表连接，肯定只有一个join条件
    realCardinality = dict()
    IntermediateList = list()
    for k in relationData.keys():
        IntermediateList.append(frozenset([k]))
        realCardinality[frozenset([k])] = relationData[k].shape[0]
    for k in g.tableNames:
        dfs(frozenset([k]), relationData[k], g, relationData, realCardinality, IntermediateList)

    for k in IntermediateList:
        logger.debug("caculateRealCardinality realCardinality of {}: {}\n", k, realCardinality[k])

    with open('./data/realCardinality/'+prefix+'.realCardinality', 'wb') as f:
        pickle.dump(realCardinality, f)




def dfs(s_in, s_in_data, g, relationData, realCardinality, IntermediateList):
    # logger.debug("sleeping...")
    # time.sleep(15)
    assert isinstance(realCardinality, dict) and isinstance(IntermediateList, list)
    if len(s_in) >= MaxJoinSize:
        return
    neighbors = g.getNeighbors(s_in)
    for nei in neighbors:
        s_out = set(s_in)
        s_out.add(nei)
        s_out = frozenset(s_out)
        if s_out in realCardinality.keys():
            continue
        #计算s_out：
        #找到和nei连接的表
        for r in s_in:
            if len(g.joinCondition[r][nei]) <= 0:
                continue
            #找到连接，做join，选择xxx_id的列
            fields = g.joinCondition[r][nei][0]
            logger.debug("join {} with {} on {}", s_in, nei, fields)
            if fields[0] == 'id':
                s_out_data = s_in_data.join(relationData[nei].set_index(nei + '.' + fields[1]), on = (r + '.' + fields[0]), how = 'inner')
                #s_out_data = pd.merge(s_in_data, relationData[nei], left_on=r + '.' + fields[0], right_on=nei + '.' + fields[1], how = "inner")
                #s_out_data = pd.concat()
                #pd.concat(df1.reset_index(drop = True), df2.reindex(df1['a'].values).reset_index(drop = Ture),axis=1)
            else:                
                s_out_data = relationData[nei].join(s_in_data.set_index(r + '.' + fields[0]), on=(nei + '.' + fields[1]), how = 'inner')
                # s_out_data = pd.merge(s_in_data, relationData[nei], left_on=r + '.' + fields[0], right_on=nei + '.' + fields[1], how = "inner")
            logger.debug("   size: {}\n", s_out_data.shape[0])
            realCardinality[s_out] = s_out_data.shape[0]
            IntermediateList.append(s_out)
            dfs(s_out, s_out_data, g, relationData, realCardinality, IntermediateList)
            del s_out_data
            gc.collect()
            break
    gc.collect()


# def caculateRealCardinality2_1(prefix, sql):
#     #检查是否已经计算过：
#     if prefix+'.realCardinality' in files:
#         logger.info("already caculated\n")
#         return

#     #获取选择执行后的单表数据
#     g = QueryGraph(sql)
#     relationData = dict()
#     for k in g.data.keys():
#         relationData[k] = g.data[k].df
#     for k in relationData.keys():
#         relationData[k] = performSelect(g, k, relationData[k])
#     del g.data
#     gc.collect()
#     #获取属性映射,1 - n
#     filedMappings = getFieldMapping(g)
#     #转化表数据，转化为多维向量和计数器的dataframe
#     for k in relationData.keys():
#         relationData[k] = relationData[k][filedMappings[k].keys()]    #只保留连接属性
#         relationData[k] = relationData[k].rename(columns = filedMappings[k]) #重命名
#         cnt = dict()
#         tmplist = list(relationData[k].values())
#         m = relationData[k].shape[1]
#         for val in relationData[k].iterrows():
#             if val[0:m] not in cnt.keys():
#                 cnt[val[0:m]] = 1
#             else cnt[val[0:m]] = cnt[val[0:m]] + 1
#         relationData[k].insert(0, 0, 1)#增加0维，代表计数，初始全为1
#     #枚举中间结果:
#     realCardinality = dict()
#     IntermediateSet = set()
#     for k in relationData.keys():
#         realCardinality[frozenset([k])] = relationData[k][0].sum()
#         IntermediateSet.add(frozenset[k])    
#     for k in relationData.keys():
#         pass

# def dfsFor2_1(s_in, s_in_data, g, relationData, realCardinality, IntermediateSet):
#     assert isinstance(realCardinality, dict) and isinstance(IntermediateSet, set)
#     neighbors = g.getNeighbors(s_in)
#     for nei in neighbors:
#         s_out = set(s_in)
#         s_out.add(nei)
#         s_out = frozenset(s_out)
#         if s_out in IntermediateSet:
#             continue
#         joinFiled = [x for x in relationData[nei].columns if x in s_in_data.columns]
#         assert len(joinFiled) == 1    
#         joinFiled = joinFiled[0]
#         df1, df2 = (s_in_data, relationData[nei]) if s_in_data.shape[0] < relationData[nei].shape[0] else (relationData[nei], s_in_data)
#         for val in df1.itertuples():
#             dftmp = df2[df2[joinFiled] == val[joinFiled]]
#             dftmp[0] = dftmp[0] * val[0]

        

    
def getFieldMapping(g):
    fieldMappings = dict()
    for tname in g.tableNames:
        fieldMappings[tname] = dict()
    #遍历边，为边上属性做映射
    now = 1
    for tname1 in g.tableNames:
        for tname2 in g.tableNames:
            for edge in g.joinCondition[tname1][tname2]:
                f1 = edge[0], f2 = edge[1]
                x, y = f1 in fieldMappings[tname1].keys(), f2 in fieldMappings[tname2].keys()
                if not x and not y:
                    fieldMappings[tname1][f1], fieldMappings[tname2][f2] = now, now
                    now = now + 1
                elif x and not y:
                    fieldMappings[tname2][f2] = fieldMappings[tname1][f1]
                elif not x and y:
                    fieldMappings[tname1][f1] = fieldMappings[tname2][f2]
    return fieldMappings
    



    

       
if __name__ == "__main__":
    logger.add(sink = "./logs/4_10_log", level="INFO")
    sqls = dict()
    with open("./data/sqls.pkl", 'rb') as f:
        sqls = pickle.load(f)
    
    for k, v in sqls.items():
        logger.info("caculateRealCardinality for {} begin...\n", k)
        #caculateRealCardinality(k, v)
        caculateRealCardinality1_1(k, v)
        gc.collect()
        logger.info("caculateRealCardinality for {} done\n", k)
        