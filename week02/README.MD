# 目录解释
jar包：

+ map-reduce ： map-reduce-code-1.0-SNAPSHOT.jar
+ hbase：hbase-code-1.0-SNAPSHOT.jar  
代码：
+ map-reduce：map-reduce-code
+ hbase：hbase-code 
# 使用
### mapreduce
执行命令：
hadoop jar map-reduce-code-1.0-SNAPSHOT-jar.jar com.mr.MrMain  $输入文件路径 $输出目录
例如hadoop jar map-reduce-code-1.0-SNAPSHOT.jar com.mr.MrMain /junyuan/HTTP_20130313143750.dat /junyuan/output
### hbase
执行命令：
java -jar hbase-code-1.0-SNAPSHOT.jar com.hbase.code.HbaseMain $命令参数
例如java -jar hbase-code-1.0-SNAPSHOT-jar-with-dependencies.jar com.hbase.code.HbaseMain scan

命令参数有如下：
1. createTable 建表（junyuan:student）
2. insert 插入数据（多条记录）
3. delete 删除个人记录(rowkey为junyuan)
4. scan 查询当前所有数据
5. query 查询单条数据（rowkey为junyuan）
6. deleteTable 删除表（junyuan:student）
