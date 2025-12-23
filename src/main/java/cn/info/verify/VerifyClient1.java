package cn.info.verify;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class VerifyClient1 {

    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final List<String> PRIMARY_KEYS = new ArrayList<>();
    private static final List<String> COMPARE_COLUMNS = new ArrayList<>();
    private static final List<String> SUM_TABLE_COLUMNS = Arrays.asList("span","cnt","sig");
    private static final int SPAN_KEY_SIZE = 4; // 16位，取MD5的前2个字节

    private static final String COMPARE_TABLE_TEMPLATE = """
    CREATE TEMPORARY TABLE %s.%s (
        compare_sign binary(16) NOT NULL,
        pk_hash binary(16) NOT NULL,
        span binary(%d) NOT NULL,
        %s,
        INDEX span_key (span, pk_hash));
    """;

    private static final String INSERT_TABLE_TEMPLATE = """
    INSERT INTO %s.%s
       (compare_sign, pk_hash, span, %s)
       SELECT
       UNHEX(MD5(CONCAT_WS('/', %s))),
       UNHEX(MD5(CONCAT_WS('/', %s))),
       UNHEX(LEFT(MD5(CONCAT_WS('/', %s)), %d)),
       %s
       FROM %s.%s;
    """;

    private static final String SUM_TABLE_TEMPLATE = """
        SELECT HEX(span) as span, COUNT(*) as cnt,
            CONCAT(SUM(CONV(SUBSTRING(HEX(compare_sign),1,8),16,10)),
            SUM(CONV(SUBSTRING(HEX(compare_sign),9,8),16,10)),
            SUM(CONV(SUBSTRING(HEX(compare_sign),17,8),16,10)),
            SUM(CONV(SUBSTRING(HEX(compare_sign),25,8),16,10))) as sig
        FROM %s.%s
        GROUP BY span
    """;

    private static final String DIFF_COMPARE = """
        SELECT * FROM %s.%s
        WHERE span = UNHEX('%s') ORDER BY pk_hash
    """;

    private static final String DIFF_COMPARE_BATCH = """
        SELECT * FROM %s.%s
            WHERE span IN (%s) ORDER BY span, pk_hash
    """;


    private static final String QUERY_COMPARE_SPAN = """
        SELECT * FROM %s.%s WHERE %s
    """;

    /**
     * 验证给定的数据表的数据一致性，并返回sql修复语句
     * 现在有几个问题：
     * 1. 当两张表的结构不同时，会导致结果出错
     * 2. 当主键不唯一时出错
     * @param conn1 生产数据库
     * @param conn2 容灾数据库
     * @param table1 生产表
     * @param table2 容灾表
     * @return {
     *     错误报告，
     *     PASS，
     *     修复语句
     * }
     */
    public String Verify(Connection conn1, Connection conn2, String table1, String table2) {


        //先连接数据库
        Statement statement1 = null;
        Statement statement2 = null;
        DatabaseMetaData dbMetaData = null;
        try {
            statement1 = conn1.createStatement();
            statement2 = conn2.createStatement();
            dbMetaData = conn1.getMetaData();
            //拿主键
            ResultSet set = dbMetaData.getPrimaryKeys(conn1.getCatalog(), conn1.getSchema(),table1);
            while (set.next()){
                PRIMARY_KEYS.add(set.getString("COLUMN_NAME"));
            }
            set.close();

            String sql1 = "select * from %s limit 1".formatted(table1);
            ResultSet resultSet1 = statement1.executeQuery(sql1);

            //拿列名
            ResultSetMetaData metaData = resultSet1.getMetaData(); //获取列集
            int columnCount = metaData.getColumnCount(); //获取列的数量

            for(int i = 1; i <= columnCount; i++){
                COMPARE_COLUMNS.add(i-1,metaData.getColumnName(i));
            }

            //创建临时比较表

            String dbName1 = conn1.getCatalog();
            String dbName2 = conn2.getCatalog();
            String compareTblName = "compare_"+table1;


            //顺序可能不重要
            List<String[]> tbl1Hash = makeSumRows(statement1, conn1.getCatalog(), compareTblName, table1);
            List<String[]> tbl2Hash = makeSumRows(statement2, conn2.getCatalog(), compareTblName, table2);

            // 计算交集，用set比list求交集快很多
            Set<String[]> set1 = new HashSet<>(tbl1Hash);
            Set<String[]> set2 = new HashSet<>(tbl2Hash);

            // 计算仅在set1中的元素
            Set<String[]> in1Not2 = new HashSet<>(set1);
            in1Not2.removeAll(set2);

            // 计算仅在set2中的元素
            Set<String[]> in2Not1 = new HashSet<>(set2);
            in2Not1.removeAll(set1);

            if(!in1Not2.isEmpty() || !in2Not1.isEmpty() ){
                List<String> tableDiffs1 = new ArrayList<>();
                List<String> tableDiffs2 = new ArrayList<>();
                for(String[] str : in1Not2){
                    tableDiffs1.add(str[0]);
                }

                for(String[] str : in2Not1){
                    tableDiffs2.add(str[0]);
                }

                Set<String> common = new HashSet<>(tableDiffs1);
                Set<String> extra2 = new HashSet<>(tableDiffs2);
                Set<String> extra1 = new HashSet<>(tableDiffs1);

                common.retainAll(extra2);
                extra1.removeAll(common);
                extra2.removeAll(common);

                List<SpanData> fullSpanData1 = new ArrayList<>();
                List<SpanData> fullSpanData2 = new ArrayList<>();

                List<Map<String,Object>> changedIn1 = new ArrayList<>();
                List<Map<String,Object>> extraIn1 = new ArrayList<>();
                List<Map<String,Object>> extraIn2 = new ArrayList<>();

                conn1.setAutoCommit(false);
                conn2.setAutoCommit(false);

                // 这里是依次查询每条结果获取数据的，后续可以考虑分批的批量查询

                // common是span的集合，span来自于pk_hash，即使span相同，pk_hash很可能是不同的，pk_hash的变动会影响行数据的hash，反之则不一定
                // mysql先是判断了行数据的hash，在判断pk_hash具体是多了，不变还是少了，如果要准确找出所有更改行，这是必须的

                if(common.size() > 0){
                    for(String str: common){
                        try(PreparedStatement statement = conn1.prepareStatement(String.format(DIFF_COMPARE,dbName1,compareTblName,str));){
                            statement.setFetchSize(100);
                            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                            try(ResultSet resultSet = statement.executeQuery();){
                                List<String[]> spanRowList = new ArrayList<>();
                                String[] spanRow = new String[3+PRIMARY_KEYS.size()];
                                Set<RowSignature> cmpSigns = new HashSet<>();
                                while(resultSet.next()){
                                    for(int i =1; i<=3+PRIMARY_KEYS.size();i++){
                                        spanRow[i-1] = resultSet.getString(i);
                                    }
                                    spanRowList.add(spanRow);
                                    RowSignature signature = new RowSignature(
                                            resultSet.getString(1),  // compare_sign
                                            resultSet.getString(2)   // pk_hash
                                    );
                                    cmpSigns.add(signature);
                                }
                                fullSpanData1.add(new SpanData(spanRowList,cmpSigns));
                            }
                        }
                    }

                    for(String str: common){
                        try(PreparedStatement statement = conn2.prepareStatement(String.format(DIFF_COMPARE,dbName2,compareTblName,str));){
                            statement.setFetchSize(100);
                            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                            try(ResultSet resultSet = statement.executeQuery();){
                                List<String[]> spanRowList = new ArrayList<>();
                                String[] spanRow = new String[3+PRIMARY_KEYS.size()];
                                Set<RowSignature> cmpSigns = new HashSet<>();
                                while(resultSet.next()){
                                    for(int i =1; i<=3+PRIMARY_KEYS.size();i++){
                                        spanRow[i-1] = resultSet.getString(i);
                                    }
                                    spanRowList.add(spanRow);
                                    RowSignature signature = new RowSignature(
                                            resultSet.getString(1),  // compare_sign
                                            resultSet.getString(2)   // pk_hash
                                    );
                                    cmpSigns.add(signature);
                                }
                                fullSpanData2.add(new SpanData(spanRowList,cmpSigns));
                            }
                        }
                    }

                    for (int pos = 0; pos < fullSpanData1.size(); pos++) {
                        SpanData spanData1 = fullSpanData1.get(pos);
                        SpanData spanData2 = fullSpanData2.get(pos);

                        // 确定表1和表2的不同行（排除未更改的行）
                        Set<RowSignature> diffRowsSign1 = calculateDifference(spanData1.getSignatures(), spanData2.getSignatures());
                        Set<RowSignature> diffRowsSign2 = calculateDifference(spanData2.getSignatures(), spanData1.getSignatures());

                        // 提取比较签名中的pk_hash
                        Set<String> diffPkHash1 = extractPkHashes(diffRowsSign1);
                        Set<String> diffPkHash2 = extractPkHashes(diffRowsSign2);

                        for(String[] res :spanData1.getRowData()){
                            if(diffRowsSign1.contains(new RowSignature(res[0],res[1]))){
                                String[] pks = Arrays.copyOfRange(res, 3, res.length);

                                StringBuilder whereClause = new StringBuilder();
                                for (int i = 0; i < PRIMARY_KEYS.size(); i++) {
                                    if (i > 0) {
                                        whereClause.append(" AND ");
                                    }
                                    whereClause.append(PRIMARY_KEYS.get(i))
                                            .append(" = '")
                                            .append(pks[i])
                                            .append("'");
                                }
                                String whereClauseStr = whereClause.toString();

                                String query = String.format(QUERY_COMPARE_SPAN,
                                        dbName1,
                                        table1,
                                        whereClauseStr);

                                try(ResultSet needChangeRes = statement1.executeQuery(query)){
                                    // 处理查询结果并分类
                                    if (needChangeRes.next()) {
                                        Map<String, Object> rowData = convertResultSetToMap(needChangeRes);

                                        if (diffPkHash2.contains(res[1])) {
                                            // 存储原始变更行（需要UPDATE）
                                            changedIn1.add(rowData);
                                        } else {
                                            // 存储原始额外行（需要insert）
                                            extraIn1.add(rowData);
                                        }
                                    }
                                }
                            }
                        }

                        for(String[] res :spanData2.getRowData()){
                            if(diffRowsSign2.contains(new RowSignature(res[0],res[1]))){
                                String[] pks = Arrays.copyOfRange(res, 3, res.length );

                                StringBuilder whereClause = new StringBuilder();
                                for (int i = 0; i < PRIMARY_KEYS.size(); i++) {
                                    if (i > 0) {
                                        whereClause.append(" AND ");
                                    }
                                    whereClause.append(PRIMARY_KEYS.get(i))
                                            .append(" = '")
                                            .append(pks[i])
                                            .append("'");
                                }
                                String whereClauseStr = whereClause.toString();

                                String query = String.format(QUERY_COMPARE_SPAN,
                                        dbName2,
                                        table2,
                                        whereClauseStr);

                                try(ResultSet needChangeRes = statement2.executeQuery(query);){
                                    // 处理查询结果并分类
                                    if (needChangeRes.next()) {
                                        Map<String, Object> rowData = convertResultSetToMap(needChangeRes);

                                        if (!diffPkHash1.contains(res[1])) {
                                            // 存储原始行（需要DELETE）
                                            extraIn2.add(rowData);
                                        }
                                    }
                                }
                            }
                        }

                    }
                }

                if(extra1.size() > 0){
                    List<Map<String, Object>> resultList = getRowSpan(table1,extra1,statement1);
                    extraIn1.addAll(resultList);
                }

                if(extra2.size() > 0){
                    List<Map<String, Object>> resultList = getRowSpan(table2,extra2,statement2);
                    extraIn2.addAll(resultList);
                }

                // 如果changedIn1不为空 表示需要 update table2 ，extraIn1 需要table2 insert， extraIn2 需要table2 delete
                if(!changedIn1.isEmpty() || !extraIn1.isEmpty() || !extraIn2.isEmpty()){
                    List<String> fixSqlStatements = generateFixSqlStatements(changedIn1, extraIn1, extraIn2,table2);

                    for(String sql : fixSqlStatements) {
                        Debug(sql);
                    }
                }


                resultSet1.close();
                statement1.close();
                statement2.close();

            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //判断checksum
//        if (CheckSum(statement1, statement2, table1, table2)){
//            return "Pass";
//        }



        return "pass";

    }


    /**
     * 根据变更数据生成SQL修复语句
     * @param changedIn1 需要更新的变更行（表1到表2）
     * @param extraIn1 需要删除的额外行（表1独有）
     * @param extraIn2 需要插入的额外行（表2独有）
     * @param table2 容灾表名
     * @return SQL修复语句列表
     */
    private List<String> generateFixSqlStatements(List<Map<String, Object>> changedIn1,
                                                  List<Map<String, Object>> extraIn1,
                                                  List<Map<String, Object>> extraIn2,
                                                  String table2) {
        List<String> fixSqlList = new ArrayList<>();

        // 生成UPDATE语句（表1变更到表2）
        for (Map<String, Object> rowData : changedIn1) {
            String updateSql = generateUpdateSql(table2, rowData);
            fixSqlList.add(updateSql);
        }

        // 生成DELETE语句（删除表2中多余的行）
        for (Map<String, Object> rowData : extraIn1) {
            String insertSql = generateInsertSql(table2, rowData);
            fixSqlList.add(insertSql);
        }

        // 生成INSERT语句（向表2插入缺少的行）
        for (Map<String, Object> rowData : extraIn2) {
            String deleteSql = generateDeleteSql(table2, rowData);
            fixSqlList.add(deleteSql);
        }

        return fixSqlList;
    }

    /**
     * 生成UPDATE SQL语句
     */
    private String generateUpdateSql(String tableName, Map<String, Object> rowData) {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(tableName).append(" SET ");

        boolean first = true;
        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();

        // 构建SET子句
        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            // 跳过主键列，主键列用于WHERE条件
            if (!PRIMARY_KEYS.contains(columnName)) {
                if (!first) {
                    setClause.append(", ");
                }
                setClause.append(columnName).append(" = ");
                if (value == null) {
                    setClause.append("NULL");
                } else {
                    setClause.append("'").append(value.toString()).append("'");
                }
                first = false;
            }
        }

        // 构建WHERE子句（使用主键）
        buildWhereClauseFromPrimaryKeys(rowData, whereClause);

        sql.append(setClause).append(" WHERE ").append(whereClause);
        return sql.toString();
    }

    /**
     * 生成DELETE SQL语句
     */
    private String generateDeleteSql(String tableName, Map<String, Object> rowData) {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(tableName);

        StringBuilder whereClause = new StringBuilder();
        buildWhereClauseFromPrimaryKeys(rowData, whereClause);

        sql.append(" WHERE ").append(whereClause);
        return sql.toString();
    }

    /**
     * 生成INSERT SQL语句
     */
    private String generateInsertSql(String tableName, Map<String, Object> rowData) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        boolean first = true;
        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            if (!first) {
                columns.append(", ");
                values.append(", ");
            }

            columns.append(entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                values.append("NULL");
            } else {
                values.append("'").append(value.toString()).append("'");
            }
            first = false;
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns.toString(), values.toString());
    }

    /**
     * 根据主键构建WHERE子句
     */
    private void buildWhereClauseFromPrimaryKeys(Map<String, Object> rowData, StringBuilder whereClause) {
        boolean first = true;
        for (String pk : PRIMARY_KEYS) {
            if (!first) {
                whereClause.append(" AND ");
            }
            Object pkValue = rowData.get(pk);
            whereClause.append(pk).append(" = ");
            if (pkValue == null) {
                whereClause.append("NULL");
            } else {
                whereClause.append("'").append(pkValue.toString()).append("'");
            }
            first = false;
        }
    }


    private List<Map<String, Object>> getRowSpan(String tableName, Set<String> extraSpans, Statement statement) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<String[]> allPkValues = new ArrayList<>();

        try {
            String cateLog = statement.getConnection().getCatalog();
            String compareTable = "compare_"+tableName;
            // 收集所有主键值
            for (String span : extraSpans) {
                String diffQuery = String.format(DIFF_COMPARE, cateLog, compareTable, span);
                try(ResultSet spanResultSet = statement.executeQuery(diffQuery);){
                    while (spanResultSet.next()) {
                        String[] pkValues = new String[PRIMARY_KEYS.size()];
                        for (int i = 0; i < PRIMARY_KEYS.size(); i++) {
                            pkValues[i] = spanResultSet.getString(4 + i);
                        }
                        allPkValues.add(pkValues);
                    }
                }
            }

            // 使用收集到的主键值查询原始表
            for (String[] pkValues : allPkValues) {
                StringBuilder whereClause = new StringBuilder();
                for (int i = 0; i < PRIMARY_KEYS.size(); i++) {
                    if (i > 0) {
                        whereClause.append(" AND ");
                    }
                    whereClause.append(PRIMARY_KEYS.get(i))
                            .append(" = '")
                            .append(pkValues[i])
                            .append("'");
                }

                String originalQuery = String.format(QUERY_COMPARE_SPAN,
                        "test3",
                        tableName,
                        whereClause.toString());

                try(ResultSet originalResultSet = statement.executeQuery(originalQuery);){
                    if (originalResultSet.next()) {
                        Map<String, Object> rowData = convertResultSetToMap(originalResultSet);
                        resultList.add(rowData);
                    }
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询额外行数据失败: " + e.getMessage(), e);
        }

        return resultList;
    }

    private Map<String, Object> convertResultSetToMap(ResultSet rs) throws SQLException {
        Map<String, Object> rowData = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();


        for (int i = 1; i <= columnCount; i++) {
            rowData.put(metaData.getColumnName(i), rs.getObject(i));
        }

        return rowData;
    }

    private Set<RowSignature> calculateDifference(Set<RowSignature> set1, Set<RowSignature> set2) {
        Set<RowSignature> difference = new HashSet<>(set1);
        difference.removeAll(set2);
        return difference;
    }

    private Set<String> extractPkHashes(Set<RowSignature> diffRows) {
        Set<String> pkHashes = new HashSet<>();
        for (RowSignature signature : diffRows) {
            pkHashes.add(signature.getPkHash());
        }
        return pkHashes;
    }

    /**
     * 1.为每个待比较表创建临时比较表
     * 2.对两张表通过INSERT语句计算行的MD5哈希值并填充比较表
     * 3.对每个表将MD5哈希值拆分为四部分进行求和，并将哈希值转换为十进制以进行数值求和。
     * 此汇总查询还会按主键哈希前4位形成的跨度列对比较表的行进行分组，最多可分为16^4 = 65536组
     * @return 汇总表
     */
    private List<String[]> makeSumRows(Statement statement,String dbName, String compareTblName, String tableName) throws SQLException {


        String pkDef = buildIndexDefinition();

        String tempSql1 = String.format(COMPARE_TABLE_TEMPLATE,
                dbName,
                compareTblName,
                SPAN_KEY_SIZE/2,
                pkDef);

        statement.execute(tempSql1);

        String pkStr = String.join(",",PRIMARY_KEYS);

        String colStr = String.join(",",COMPARE_COLUMNS);

        tempSql1 = String.format(INSERT_TABLE_TEMPLATE,
                dbName,
                compareTblName,
                pkStr,
                colStr,
                pkStr,
                pkStr,
                SPAN_KEY_SIZE,
                pkStr,
                dbName,
                tableName);

        statement.execute(tempSql1);

        tempSql1 = String.format(SUM_TABLE_TEMPLATE, dbName, compareTblName);

        try (PreparedStatement preparedStatement = statement.getConnection().prepareStatement(tempSql1)) {
            preparedStatement.setFetchSize(1000);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String[]> objectList = new ArrayList<>();

                while (resultSet.next()) {
                    String[] objects = new String[3];
                    for (int i = 0; i < 3; i++) {
                        objects[i] = resultSet.getString(i + 1);
                    }
                    objectList.add(objects);
                }

                return objectList;
            }
        }
    }

    private String buildIndexDefinition() {
        StringBuilder indexDefn = new StringBuilder();

        for (String primaryKey : PRIMARY_KEYS) {
            indexDefn.append(primaryKey)
                    .append(" VARCHAR(255), ");  // 假设默认类型，实际应根据具体字段类型确定
        }

        // 移除最后的逗号和空格
        if (indexDefn.length() > 0) {
            indexDefn.setLength(indexDefn.length() - 2);
        }

        return indexDefn.toString();
    }


    private void MulQuery(StringBuilder sql, String obj) {
        String[] pkValues = obj.split("\\|");
        StringBuilder whereClause = new StringBuilder(" WHERE ");
        for (int i = 0; i < PRIMARY_KEYS.size(); i++) {
            if (i > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(PRIMARY_KEYS.get(i)).append(" = '").append(pkValues[i]).append("'");
        }
        sql.append(whereClause).append(";");
    }

    /**
     * 比较数据库的对象差异，并返回sql修复语句
     * @param conn1 生产数据库
     * @param conn2 容灾数据库
     * @return {
     *     错误报告，
     *     PASS，
     *     修复语句
     * }
     */
    public String CompareDb(Connection conn1, Connection conn2){

        try{

            MySQLDatabaseInspector mySQLDatabaseInspector1 = new MySQLDatabaseInspector(conn1,"test1");
            MySQLDatabaseInspector.ObjectResult objectList1 = mySQLDatabaseInspector1.getDatabaseObjects(MySQLDatabaseInspector.ObjectType.TABLE, MySQLDatabaseInspector.ColumnMode.BRIEF,false);
            MySQLDatabaseInspector mySQLDatabaseInspector2 = new MySQLDatabaseInspector(conn2,"test2");
            MySQLDatabaseInspector.ObjectResult objectList2 = mySQLDatabaseInspector2.getDatabaseObjects(MySQLDatabaseInspector.ObjectType.TABLE, MySQLDatabaseInspector.ColumnMode.BRIEF,false);



            // 2.比较两个库的对象,得到两个库共有的对象和各自独有的对象
            if(objectList1.data().size()!=objectList2.data().size()){

            }

        }catch (SQLException e){
            Error("数据库查询错误:"+e.getMessage());
        }

        // 3.获取两个库的对象的差异

        // 4.根据（3）中对象的差异类型生成sql语句

        // 5.对生成语句的检查，排除语句顺序对影响的误判，以及空语句的排查。
        return "PASS";
    }

    /**
     * 判断CheckSum是否相同
     */
    private boolean CheckSum(Statement statement1, Statement statement2, String table1, String table2) throws SQLException {

        String checkSql1 = "checksum table %s".formatted(table1);
        String checkSql2 = "checksum table %s".formatted(table2);

        long checksum1 = 0;
        long checksum2 = 0;

        ResultSet rs1 = statement1.executeQuery(checkSql1);
        ResultSet rs2 = statement2.executeQuery(checkSql2);
        while (rs1.next()) {
            checksum1 = rs1.getLong("Checksum");
        }
        while (rs2.next()) {
            checksum2 = rs2.getLong("Checksum");
        }
        rs1.close();
        rs2.close();

        return checksum1 == checksum2;

    }

    private static <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(list.subList(i, end));
        }
        return batches;
    }

    private Map<String, List<String>> getDatabaseObjects(Connection conn, String databaseName) throws SQLException {
        Map<String, List<String>> objects = new HashMap<>();

        DatabaseMetaData metaData = conn.getMetaData();

        // 获取表和视图
        ResultSet tables = metaData.getTables(databaseName, null, null, new String[]{"TABLE", "VIEW"});
        List<String> tableList = new ArrayList<>();
        while (tables.next()) {
            tableList.add(tables.getString("TABLE_NAME"));
        }
        objects.put("tables", tableList);
        tables.close();

        // 获取存储过程
        ResultSet procedures = metaData.getProcedures(databaseName, null, null);
        List<String> procedureList = new ArrayList<>();
        while (procedures.next()) {
            procedureList.add(procedures.getString("PROCEDURE_NAME"));
        }
        objects.put("procedures", procedureList);
        procedures.close();

        // 获取函数
        ResultSet functions = metaData.getFunctions(databaseName, null, null);
        List<String> functionList = new ArrayList<>();
        while (functions.next()) {
            functionList.add(functions.getString("FUNCTION_NAME"));
        }
        objects.put("functions", functionList);
        functions.close();

        return objects;
    }


    private void Debug(String obj){
        System.out.println(obj);
    }

    private void Error(String obj){
        System.err.println(obj);
    }
}
