import re
import glob
import pandas as pd
from loguru import logger
#用于sql解析的字符串常量
s_select = 'select'
s_from = 'from'
s_where = 'where'
s_value = 'value'
s_name = 'name'
s_literal = 'literal'
s_is_null = 'missing'
s_is_not_null = 'exists'
s_between = 'between'
s_like = 'like'
s_greater = 'gt'
s_lt = 'lt'
s_gt = 'gt'
s_eq = 'eq'
s_neq = 'neq'
s_neq = 'neq'
s_or = 'or'
s_and = 'and'
s_like = 'like'
s_not_like = 'not_like'
s_in = 'in'

#从csv采样的比例，将大表的数据行数采样到500,000的样子
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


data_loc = 'data/'
pkl_loc = data_loc + 'pkl/'
#载入pkl数据
def load_pickle(name):
    """ Load the given CSV file only """
    df = pd.read_pickle(pkl_loc + name + '.pkl')
    return df

#给一个parse结果，返回表名映射 
def GetTableMapping(p):
    assert(isinstance(p, dict)) and s_from in p.keys() and isinstance(p[s_from], list)
    tableMapping = dict()
    for v in p[s_from]:
        if isinstance(v, dict):
            #有重命名
            tableMapping[v[s_name]] = v[s_value]
        else:
            assert isinstance(v, str)
            tableMapping[v] = v

    return tableMapping

#判断parse结果中‘eq'对应的list[‘ ’ , ‘ ’]是不是连接语句
def IsJoinDes(x):
    if isinstance(x, list) and len(x) == 2 and isinstance(x[0], str) and isinstance(x[1], str) \
        and re.match(".*\..*", x[0]) and re.match(".*\..*", x[1]) and x[0].split('.')[0] != x[1].split('.')[0]:
        return True
    else:
        return False
#判断字符串是否是‘and' 'or'
def isAndOr(x):
    assert isinstance(x, str)
    return x == s_and or x == s_or

#将job查询转换成不带分号的一行字符串写入query_sql_processed
def processJOBsql():
    outFile = "./data/query_sql_processed"
    sqlFilePath = "./data/join-order-benchmark/"
    sqlFileMatch = sqlFilePath + "/[0-9]*.sql"
    files = glob.glob(sqlFileMatch)
    with open(outFile, 'w') as outfile:
        for file in files:
            with open(file, 'r') as f:
                sql = f.read().replace("\n", " ").replace(";","")
                outfile.write(sql+"\n")  

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

if __name__ == "__main__":
    loadPklWithSample()
        
    
    


            


    
