# mysql存储对象的python实现:
1. 通过_get_objects得到数据库的所有对象，包括数据表，存储过程，视图等。
    ```db_obj = Database(server, database, options)```
    其中这一句就能直接拿到。这里的的Database方法来自于mysql-utilities的database.py
2. 比较两个库的对象,得到两个库共有的对象和各自独有的对象
   ```
   s1 = set(list1)
   s2 = set(list2)
   both = s1 & s2
   return(list(both), list(s1 - both), list(s2 - both))
    ```
    这一块python里的写法是十分简洁的。
3. 获取两个库的对象的差异，是通过python的difflib库来实现的。
4. 调用sql_transform.py的transform_definition类,根据（3）中对象的差异类型生成sql语句
    ``` 
        statement_parts = [
            # preamble
            {'fmt': "%s", 'col': _IGNORE_COLUMN, 'val': "ALTER TABLE"},
            # object name
            {'fmt': " %s.%s", 'col': _IGNORE_COLUMN,
             'val': (q_dest_db_name, q_dest_tbl_name)},
            # alter clauses - will be completed later
            {'fmt': " \n%s", 'col': _IGNORE_COLUMN, 'val': ""},
        ]
    ```
   这是创建表的语句，其中包含数据库名和数据表名。外键，行，索引在后续的方法中得到并拼接，再与这里的语句拼接成完整的sql语句。创建存储过程等对象的方法类似。
5. 对生成语句的检查，排除语句顺序对影响的误判，以及空语句的排查。


# mysql数据行的比较算法:
(程序在输入的时候可以配置，如果包含no_data则忽略对数据一致性的校验)
原始表
↓
计算每行的MD5哈希
↓
插入临时表（保存所有哈希，主键值）
↓
分组计算汇总（COUNT, SUM）
↓
比较汇总结果（少量数据）
↓
获取详细行数据（按span组）
1. 计算比较checksum，二者相同直接pass。mysql和oracle支持这个方法。

2. 获取使用的索引，去除索引的引号。    我直接用的主键

3. 若客户端二进制日志开启（sql_log_bin==1），则将其关闭。    没改二进制日志

4. 为每个待比较表创建临时比较表。      
    ```
    CREATE TEMPORARY TABLE {db}.{compare_tbl} (
        compare_sign binary(16) NOT NULL,
        pk_hash binary(16) NOT NULL,
        {pkdef}
        span binary({span_key_size}) NOT NULL,
        INDEX span_key (span, pk_hash)) ENGINE=MyISAM
   ```
    没创临时表，强制使用主键判断
    得到两张表的非空且唯一索引的列表，若用户提供索引，判断用户提供的索引是否在这两张表中，否则使用主键。生成包含数据库名称，比较表名称，主键定义，跨度键大小的临时比较表数据。
    将表的autocommit设为1
5. 对两张表通过INSERT语句计算行的MD5哈希值并填充比较表。      没设置锁，直接全部读到内存中操作
    为原表设置读锁，为临时比较表设置写锁，使用设置的模板生成比较表名称和insert语句，insert语句中包含数据库名称，比较表名称，列名字符串，主键字符串，原表名称，跨度键大小。
   ```
   INSERT INTO {db}.{compare_tbl}
   (compare_sign, pk_hash, {pkstr}, span)
   SELECT
   UNHEX(MD5(CONCAT_WS('/', {colstr}))),
   UNHEX(MD5(CONCAT_WS('/', {pkstr}))),
   {pkstr},
   UNHEX(LEFT(MD5(CONCAT_WS('/', {pkstr})), {span_key_size}))
   FROM {db}.{table}
   ```
6. 对每个表将MD5哈希值拆分为四部分进行求和，并将哈希值转换为十进制以进行数值求和。此汇总查询还会按主键哈希前4位形成的跨度列对比较表的行进行分组，最多可分为16^4 = 65536组。
    ```
   SELECT HEX(span), COUNT(*) as cnt,
    CONCAT(SUM(CONV(SUBSTRING(HEX(compare_sign),1,8),16,10)),
    SUM(CONV(SUBSTRING(HEX(compare_sign),9,8),16,10)),
    SUM(CONV(SUBSTRING(HEX(compare_sign),17,8),16,10)),
    SUM(CONV(SUBSTRING(HEX(compare_sign),25,8),16,10))) as sig
    FROM {db}.{compare_tbl}
    GROUP BY span
   ```
   解开步骤（5）中的锁
7. 使用集合方法比较汇总表，找出同时出现在两个表中的行（跨度）、仅出现在表1的行以及仅出现在表2的行。若集合操作中行不匹配，则意味着该跨度的哈希总和不同，即该跨度中有一行或多行在另一表中缺失或数据不同。若无差异，则跳至步骤(9)。
   ```
   s1 = set(list1)
   s2 = set(list2)
   both = s1 & s2
   return(list(both), list(s1 - both), list(s2 - both))
    ```
8. 对包含差异行的跨度值再次使用集合操作进行比较。同时出现在两个集合中的跨度包含已更改的行，而仅出现在某一集合中的行（反之亦然）包含缺失的行。
   若表密度足够大，已更改行的跨度中可能包含缺失行。这仍可接受，因为差异输出仍会将数据显示为缺失。
    我不太理解为什么下面这两次get_common_list能分别对跨度和跨度内的行比较
   ```
    _, in1_not2, in2_not1 = get_common_lists(tbl1_hash, tbl2_hash)

    if len(in1_not2) != 0 or len(in2_not1) != 0:
        table1_diffs = []
        table2_diffs = []

        # Get keys for diffs on table1
        for row in in1_not2:
            table1_diffs.append(row[0])

        # Get keys for diffs on table2
        for row in in2_not1:
            table2_diffs.append(row[0])

        # Find changed and missing rows
        changed_rows, extra1, extra2 = get_common_lists(table1_diffs,
                                                        table2_diffs)
    ```
9. 将步骤(6)中相同跨度的输出用于生成差异。
10. 将步骤(7)中包含缺失跨度的输出格式化为结果列表以供用户查看。
    通过下面的语句获取指定span值的所有比较行数据，并将compare_sign和pk_hash建立集合
    ```
    SELECT * FROM {db}.{compare_tbl}
    WHERE span = UNHEX('{span}') ORDER BY pk_hash
    ```
    获取差异行，并获取差异行元组的第二个元素pk_hash
    ```
    diff_rows_sign1 = span_data1[1] - span_data2[1]
    diff_rows_sign2 = span_data2[1] - span_data1[1]
    diff_pk_hash1 = set(cmp_sign[1] for cmp_sign in diff_rows_sign1)
    diff_pk_hash2 = set(cmp_sign[1] for cmp_sign in diff_rows_sign2)
    ```
    构建where语句，通过pk查询原始的行数据，如果res_row1(pk_hash)存在于diff_pk_hash2中则是变更的数据，否则是新增的数据
    ```
    where_clause = ' AND '.join("{0} = '{1}'".
            format(key, col)
            for key, col in zip(ukeys, pk))
    
    if res_row[1] in diff_pk_hash2:
        # Store original changed row (to UPDATE).
        changed_in1.append(res[0])
    ```
    通过difflib工具类比较上面提取出的行数据的差异，对于changed_row调用sql_transform类去添加数据库的update语句，extra_row根据需求添加insert或delete语句。
    
11. 销毁比较数据库，(None, None)的返回值表示数据一致。
12. 若步骤(1)中关闭了二进制日志，则重新将其开启。

# 抽样算法：
1. 从生产库抽样 用啥抽样算法，抽多少量
2. 