package cn.info.verify;

import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class VerifyClient {

    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final List<String> PRIMARY_KEYS = Arrays.asList("id");
    private static final List<String> COMPARE_COLUMNS = Arrays.asList("name", "id", "phone", "age");
    private static final int SPAN_KEY_SIZE = 16; // 16位，取MD5的前2个字节

    public String Verify(Connection conn1, Connection conn2, String table1, String table2) throws SQLException {

        //先连接数据库
        Statement statement1 = conn1.createStatement();
        Statement statement2 = conn2.createStatement();

        //拿主键
        //catalog指定库的名字，需要用户输入或者从url中读取
        DatabaseMetaData dbMetaData = conn1.getMetaData();
        ResultSet set = dbMetaData.getPrimaryKeys("test1","test1",table1);
        int count = 0;
        while (set.next()){
            PRIMARY_KEYS.set(count++,set.getString("COLUMN_NAME"));
        }
        set.close();

        //判断checksum
        if (CheckSum(statement1, statement2, table1, table2)){
            return "Pass";
        }

        String sql1 = "select * from "+table1;
        ResultSet resultSet1 = statement1.executeQuery(sql1);

        String sql2 = "select * from "+table2;
        ResultSet resultSet2 = statement2.executeQuery(sql2);

        //取数据,并将数据处理成compareTable的格式（大数据量的话这会不会很慢呢,而且会占用很多内存

        List<Map<String,Object>> list1 = new ArrayList<>();
        List<Map<String, Object>> allMaps = new ArrayList<>();
        //拿列名
        ResultSetMetaData metaData = resultSet1.getMetaData(); //获取列集
        int columnCount = metaData.getColumnCount(); //获取列的数量

        for(int i = 1; i <= columnCount; i++){
            COMPARE_COLUMNS.set(i-1,metaData.getColumnName(i));
        }

        //这里可以考虑先批量处理map1再addAll
        while (resultSet1.next()) {
            Map<String,Object> map1 = new HashMap<>((int)(columnCount / 0.75f) + 1);
            for(int i = 1 ;i<=columnCount;i++){
                map1.put(COMPARE_COLUMNS.get(i-1),resultSet1.getString(i));
            }
            list1.add(map1);
            allMaps.add(map1);
        }

        // 并行转换
        List<CompareTable> transformedList = allMaps.parallelStream()
                .map(this::transForm)
                .toList();

        allMaps.clear();
        List<CompareTable> compareTableList1 = new ArrayList<>(transformedList);

        //不需要获取list2的原数据
        //List<Map<String,Object>> list2 = new ArrayList<>();
        metaData = resultSet2.getMetaData(); //获取列集
        columnCount = metaData.getColumnCount(); //获取列的数量
        while (resultSet2.next()) {
            Map<String,Object> map2 = new HashMap<>();
            for(int i = 1 ;i<=columnCount;i++){
                map2.put(COMPARE_COLUMNS.get(i-1),resultSet2.getString(i));
            }
            allMaps.add(map2);
        }
        // 并行转换
        List<CompareTable> transformedList2 = allMaps.parallelStream()
                .map(this::transForm)
                .toList();

        List<CompareTable> compareTableList2 = new ArrayList<>(transformedList2);


        //todo 这里使用的是唯一的主键，如果主键不唯一还需要处理
        Map<String,Map<String,Object>> index1 = createIndex(list1,PRIMARY_KEYS.get(0),map -> (String) map.get(PRIMARY_KEYS.get(0)) );
        //这里假定table1是主库，不关心table2的数据
//        Map<String,Map<String,Object>> index2 = new HashMap<>();
//        index2 = createIndex(list2,"id",map -> (String) map.get("id") );

        //关闭连接，用不到了
        resultSet1.close();
        statement1.close();
        resultSet2.close();
        statement2.close();

        //如果把上面的compareTable插入到数据库里，这里通过order by得到分组结果也是很方便的，后续还有通过span反找compareTableList的pkValue在代码上是简单的
        List<HashSummaryTable> hashSummaryTableList1 = calculateSummary(compareTableList1);
        List<HashSummaryTable> hashSummaryTableList2 = calculateSummary(compareTableList2);

        // 计算交集，用set比list求交集快很多
        Set<HashSummaryTable> set1 = new HashSet<>(hashSummaryTableList1);
        Set<HashSummaryTable> set2 = new HashSet<>(hashSummaryTableList2);

        // 计算仅在set1中的元素
        Set<HashSummaryTable> in1Not2 = new HashSet<>(set1);
        in1Not2.removeAll(set2);

        // 计算仅在set2中的元素
        Set<HashSummaryTable> in2Not1 = new HashSet<>(set2);
        in2Not1.removeAll(set1);

        // 先根据这里找到的span获取原始的pk列表
        Set<String> pkSet1 = new HashSet<>();
        // 创建两个集合的span集合
        Set<String> compareSpans = new HashSet<>(compareTableList1.size());
        Map<String, CompareTable> spanToCompareTableMap = new HashMap<>(compareTableList1.size());
        for (CompareTable compareTable : compareTableList1) {
            String span = compareTable.getSpan();
            compareSpans.add(span);
            spanToCompareTableMap.put(span, compareTable);
        }

        // 只处理存在于两个集合中的span
        for (HashSummaryTable table : in1Not2) {
            String span = table.getSpan();
            if (compareSpans.contains(span)) {
                CompareTable ct = spanToCompareTableMap.get(span);
                Map<String, Object> pkValues = ct.getPkValues();
                if (pkValues != null) {
                    for (Object pkValue : pkValues.values()) {
                        if (pkValue != null) {
                            pkSet1.add(pkValue.toString());
                        }
                    }
                }
            }
        }

        Set<String> pkSet2 = new HashSet<>();

        compareSpans = new HashSet<>(compareTableList2.size());
        spanToCompareTableMap = new HashMap<>(compareTableList2.size());
        for (CompareTable compareTable : compareTableList2) {
            String span = compareTable.getSpan();
            compareSpans.add(span);
            spanToCompareTableMap.put(span, compareTable);
        }

        // 只处理存在于两个集合中的span
        for (HashSummaryTable table : in2Not1) {
            String span = table.getSpan();
            if (compareSpans.contains(span)) {
                CompareTable ct = spanToCompareTableMap.get(span);
                Map<String, Object> pkValues = ct.getPkValues();
                if (pkValues != null) {
                    for (Object pkValue : pkValues.values()) {
                        if (pkValue != null) {
                            pkSet2.add(pkValue.toString());
                        }
                    }
                }
            }
        }

        //重复上面的操作，找pkSet1和pkSet2的并集和差集
        Set<String> common = new HashSet<>(pkSet1);
        common.retainAll(pkSet2);

        // 计算仅在set1中的元素
        Set<String> extra1 = new HashSet<>(pkSet1);
        extra1.removeAll(pkSet2);

        // 计算仅在set2中的元素
        Set<String> extra2 = new HashSet<>(pkSet2);
        extra2.removeAll(pkSet1);

        StringBuilder sql = new StringBuilder();
        //按照pkSet1中的id的行数据对set2的数据执行update操作
        //这里按这种写法应该有性能和安全问题
        if(!common.isEmpty()){
            for(String obj: common){
                Map<String,Object> map = index1.get(obj);
                sql.append("UPDATE ").append(table2).append(" SET ");
                map.forEach((key, value) -> {
                    sql.append(key).append(" = '").append(value).append("',");
                });
                //删除末尾的逗号
                sql.deleteCharAt(sql.length()-1);
                sql.append(" WHERE ").append("id").append(" = '").append(obj).append("';");
            }
        }
        //执行insert
        if(!extra1.isEmpty()){
            sql.append("INSERT INTO ").append(table2).append(" (");
            StringBuilder keySql = new StringBuilder();
            StringBuilder valueSql = new StringBuilder();
            Map<String, Object> map;

            //先拼接列名
            String str = extra1.iterator().next();
            map = index1.get(str);
            map.forEach((key,value) -> {
                keySql.append(key).append(",");
            });
            // 删除末尾多余的逗号
            if (valueSql.length() > 0) {
                valueSql.deleteCharAt(valueSql.length() - 1);
            }

            sql.append(valueSql).append(") VALUES ");
            //sql.append(String.join(",",COMPARE_COLUMNS)).append(") VALUES ");

            //再拼接values
            for(String obj: extra1){
                map = index1.get(obj);

                valueSql.append("(");
                map.forEach((key,value) -> {
                    valueSql.append("'").append(value).append("',");
                });

                // 删除末尾多余的逗号
                if (valueSql.length() > 0) {
                    valueSql.deleteCharAt(valueSql.length() - 1);
                }
                valueSql.append("),");
            }
            if (valueSql.length() > 0) {
                valueSql.deleteCharAt(valueSql.length() - 1);
            }

            sql.append(valueSql).append(";");
        }
        //执行delete
        if(!extra2.isEmpty()){
            for(String obj: extra2){
                sql.append("DELETE FROM ").append(table2);
                sql.append(" WHERE ").append("id").append(" = '").append(obj).append("';");
            }
        }
        return sql.toString();
    }

    private CompareTable transForm(Map<String, Object> data){
        try {
            // 生成所有字段的compare_sign
            String compareSign = generateCompareSign(data);

            // 生成主键字段的pk_hash,这个属性后续用不到了
            String pkHash = generatePkHash(data);

            // 提取主键字段值
            Map<String, Object> pkValues = extractPkValues(data);

            // 生成span
            String span = pkHash.substring(0, Math.min(SPAN_KEY_SIZE, pkHash.length()));
            return new CompareTable(compareSign, pkValues, span);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法不可用", e);
        }

    }

    /**
     * 对数据的全量比较
     * @param list1
     * @param list2
     */
    private void CompareData(List<Map<String,Object>> list1, List<Map<String,Object>> list2) {

        //数据的处理，得到类似于mysql中的comparetable的东西


    }

    /**
     * 判断CheckSum是否相同
     * @param statement1
     * @param statement2
     * @param table1
     * @param table2
     */
    private boolean CheckSum(Statement statement1, Statement statement2, String table1, String table2) throws SQLException {

        String checkSql1 = "checksum table "+table1;
        String checkSql2 = "checksum table "+table2;

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

    private static String generateCompareSign(Map<String, Object> data) throws NoSuchAlgorithmException {

        StringBuilder concat = new StringBuilder();
        for (String column : VerifyClient.COMPARE_COLUMNS) {
            Object value = data.get(column);
            if (value != null) {
                concat.append(value);
            }
        }
        return md5ToHex(concat.toString());
    }

    private static String md5ToHex(String input) throws NoSuchAlgorithmException {
        //MessageDigest md = MessageDigest.getInstance("MD5");
        //用单例模式能更快吗
        MessageDigest md = MD5_DIGEST.get();
        byte[] digest = md.digest(input.getBytes(UTF_8));
        return bytesToHex(digest);
    }

    // ThreadLocal 缓存 MessageDigest 实例
    private static final ThreadLocal<MessageDigest> MD5_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });

    //将原先使用的String.format改为了从字符数组索引，并用位运算计算下标
    private static String bytesToHex(byte[] bytes) {
        final int length = bytes.length;
        final char[] hexChars = new char[length << 1];
        for (int i = 0; i < length; i++) {
            hexChars[i << 1] = HEX_ARRAY[(0xF0 & bytes[i]) >>> 4];
            hexChars[(i << 1) + 1] = HEX_ARRAY[0x0F & bytes[i]];
        }
        return new String(hexChars);
    }

    private static String generatePkHash(Map<String, Object> data)
            throws NoSuchAlgorithmException {

        StringBuilder concat = new StringBuilder();
        for (String pk : VerifyClient.PRIMARY_KEYS) {
            Object value = data.get(pk);
            if (value != null) {
                concat.append(value);
            }
        }

        return md5ToHex(concat.toString());
    }

    /**
     * 提取主键字段值
     */
    private static Map<String, Object> extractPkValues(Map<String, Object> data) {
        Map<String, Object> pkValues = new LinkedHashMap<>();
        for (String pk : VerifyClient.PRIMARY_KEYS) {
            pkValues.put(pk, data.get(pk));
        }
        return pkValues;
    }

    public static List<HashSummaryTable> calculateSummary(List<CompareTable> compareTableList) throws SQLException {

        Map<String, GroupData> groupMap = new HashMap<>();

        for (CompareTable compareTable : compareTableList) {
            String spanHex = compareTable.getSpan();
            String compareSignHex = compareTable.getCompareSign();

            // 获取或创建分组
            GroupData group = groupMap.computeIfAbsent(spanHex,
                    k -> new GroupData(spanHex));

            // 更新分组统计
            group.incrementCount();

            // 计算四部分的和
            Long[] parts = splitAndSumHash(compareSignHex);
            group.addToSums(parts);
        }

        // 转换为HashSummaryTable
        return groupMap.values().stream()
                .map(group -> HashSummaryTable.builder()
                        .span(group.spanHex)
                        .count(group.count)
                        .sumPart1(group.sum1)
                        .sumPart2(group.sum2)
                        .sumPart3(group.sum3)
                        .sumPart4(group.sum4)
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * 使用Map建立索引：主键 -> Map对象
     */
    public static <T> Map<T, Map<String, Object>> createIndex(
            List<Map<String, Object>> list,
            String propertyName,
            Function<Map<String, Object>, T> valueExtractor) {

        Map<T, Map<String, Object>> index = new HashMap<>();
        for (Map<String, Object> map : list) {
            if (map.containsKey(propertyName)) {
                T key = valueExtractor.apply(map);
                index.put(key, map);
            }
        }
        return index;
    }

    /**
     * 将MD5哈希值拆分为四部分并转换为十进制
     */
    private static Long[] splitAndSumHash(String md5Hex) {
        // MD5是32个十六进制字符，分为4部分，每部分8个字符
        if (md5Hex.length() != 32) {
            throw new IllegalArgumentException("MD5哈希值必须是32个字符: " + md5Hex);
        }

        Long[] parts = new Long[4];

        // 直接操作字符数组，避免substring和Long.parseLong
        char[] chars = md5Hex.toCharArray();

        parts[0] = parseHexChars(chars, 0, 8);
        parts[1] = parseHexChars(chars, 8, 16);
        parts[2] = parseHexChars(chars, 16, 24);
        parts[3] = parseHexChars(chars, 24, 32);

        return parts;
    }

    private static long parseHexChars(char[] chars, int start, int end) {
        long result = 0;
        for (int i = start; i < end; i++) {
            char c = chars[i];
            result = (result << 4) | hexCharToInt(c);
        }
        return result;
    }

    private static int hexCharToInt(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        throw new IllegalArgumentException("非十六进制字符: " + c);
    }

    private void Debug(String obj){
        System.out.println(obj);
    }
}
