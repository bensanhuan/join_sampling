# join_sampling
TODO：

数据准备：
[√]job-sql语句集合到一张表
[√] 生成完整的db表对应dataframe
[ ] DB表上索引补全

算法开发：
1. Class QueryGraph：
[√] 从sql中构造joinCondition
[√] 逻辑优化joinCodition，并可视化queryGraph检查是不是都是树状
[ ] 从queryGraph图描述中，实现get_neighbors方法，返回与节点相邻的节点列表
[√] 单表选择下推并执行
[ ] fix原来的逻辑，把单表选择做成filter，不提前执行

2. IndexSample算法执行函数：
[ ] 实现方案一采样过程
[ ] 构建filed和统计维度的映射关系（n:1)，实现merge算法，快速计算将两个统计信息连接后的统计信息
[ ] queryGraph上自顶向下为每个表采样，并记录统计信息
[ ] 根据中间结果获取最佳执行计划（算法待调研）

