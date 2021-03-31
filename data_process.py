from QueryGraph import *
from loguru import logger
import matplotlib.pyplot as plt
import networkx as nx
import glob
import pandas as pd
import pickle

preSamplePercentages = {
    'cast_info': 0.0135,
    'movie_info': 0.03,
    'movie_keyword': 0.11,
    'name': 0.12,
    'char_name': 0.16,
    'person_info': 0.175,
    'movie_companies': 0.19,
    'title': 0.2,
    'move_info_idx':0.38,
    'aka_name': 0.55
}


pkl_sampled_loc = 'data/pkl/'
pkl_unSampled_loc = 'data/pkl-unSample/'
#载入pkl数据
def load_pickle(name, useSampledPkl):
    """ Load the given CSV file only """
    if useSampledPkl:
        df = pd.read_pickle(pkl_sampled_loc + name + '.pkl')
    else:
        df = pd.read_pickle(pkl_unSampled_loc + name + '.pkl')
    return df

#将csv原数据导入pkl数据，由于数据过多，先提前进行采样
def loadPklWithSample():
    #load csv
    #从csv_schema.txt解析csv文件格式
    columns = dict()
    with open('./data/csv_schema.txt', 'r') as f:
        lines = f.readlines()
        for l in lines:
            l = l.replace('\n', '').split(',')
            columns[l[0]] = l[1:]
    logger.info("columns: {}", columns)
    csvPath = "./data/csv/"
    pklPath = "./data/pkl/"
    for k, v in columns.items():
        logger.info("begin to create pkl for {}", k)
        df = pd.read_csv(csvPath + k + '.csv', header = None, escapechar = '\\', names = v)
        if k in preSamplePercentages.keys():
            #执行预采样 
            logger.info("orgin size: {} ,sample goal size: {}", df.shape[0],int(preSamplePercentages[k] * df.shape[0]))
            df = df.sample(int(preSamplePercentages[k] * df.shape[0]))
        #存储
        df.to_pickle(pklPath + k + '.pkl')
        logger.info("{}.pkl has saved", k)

#我内存好像完全hold的住
def loadPklWithoutSample():
    #load csv
    #从csv_schema.txt解析csv文件格式
    columns = dict()
    with open('./data/csv_schema.txt', 'r') as f:
        lines = f.readlines()
        for l in lines:
            l = l.replace('\n', '').split(',')
            columns[l[0]] = l[1:]
    logger.info("columns: {}", columns)
    csvPath = "./data/csv/"
    pklPath = "./data/pkl-unSample/"
    for k, v in columns.items():
        logger.info("begin to create pkl for {}", k)
        df = pd.read_csv(csvPath + k + '.csv', header = None, escapechar = '\\', names = v)
        #存储
        df.to_pickle(pklPath + k + '.pkl')
        logger.info("{}.pkl has saved", k)

#试一下占用多少内存
def loadAllPkl():
    columns = dict()
    with open('./data/csv_schema.txt', 'r') as f:
        lines = f.readlines()
        for l in lines:
            l = l.replace('\n', '').split(',')
            columns[l[0]] = l[1:]
    logger.info("columns: {}", columns)
    pklPath = "./data/pkl-unSample/"
    dfs = list()
    for k, _ in columns.items():
        dfs.append(pd.read_pickle(pklPath + k + ".pkl"))
    _ = input("load done....")

#绘制queryGraph图片，用于观察查询结构。结论：优化后都是树状
def paintGraphForSql():
    sql_dir="data/join-order-benchmark"
    files_match = sql_dir+"/[0-9]*.sql"
    files = glob.glob(files_match)
    for file in files:
        with open(file, 'r') as f:
            sql = f.read().replace("\n", " ")
            logger.info("begin draw for {}", sql)
            ss = file.split('/')[2].split('.')[0]
            sql = sql.replace(';','')
            qg = QueryGraph(sql, True)
            plt.clf()
            g = nx.Graph()
            nameid = dict()
            id = 1
            for name in qg.tableNames:
                nameid[name] = id
                g.add_node(id)
                id += 1
            for aname in qg.tableNames:
                for bname in qg.tableNames:
                    for x in qg.joinCondition[aname][bname]:
                        g.add_edge(nameid[aname], nameid[bname])
            nx.draw(g, with_labels = True)
            plt.savefig('./data/query_graph_img/qg_' + ss + '.png')
            logger.info("save {},img", ss)


#将job查询转换成不带分号的一行字符串写入sqls.pkl
#dict[file_prefix] = str(sql)
def processJOBsql():
    #outFile = "./data/query_sql_processed"
    sqlFilePath = "./data/join-order-benchmark/"
    sqlFileMatch = sqlFilePath + "/[0-9]*.sql"
    files = glob.glob(sqlFileMatch)
    sqlsPkl = dict()
    #with open(outFile, 'w') as outfile:
    for file in files:
        with open(file, 'r') as f:
            sql = f.read().replace("\n", " ").replace(";","")
            sqlsPkl[file.split('/')[3].split('.')[0]] = sql
            #outfile.write(sql+"\n")  
    logger.info("dict: {}", sqlsPkl)
    with open('./data/sqls.pkl', 'wb') as f:
        pickle.dump(sqlsPkl, f)


if __name__ == "__main__":
    processJOBsql()          
