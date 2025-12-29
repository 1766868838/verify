package cn.infocore.dbs.compare.verify;

import com.github.difflib.DiffUtils;
import com.github.difflib.UnifiedDiffUtils;
import com.github.difflib.patch.Patch;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

@Component
public class DbCompareUtils {

    private final static int DEFAULT_SPAN_KEY_SIZE = 4;
    private static final List<String> PRIMARY_KEYS = new ArrayList<>();
    private static final List<String> COMPARE_COLUMNS = new ArrayList<>();

    private static final String COMPARE_TABLE = """
                CREATE TEMPORARY TABLE %s.%s (
                    compare_sign binary(16) NOT NULL,
                    pk_hash binary(16) NOT NULL,
                    span binary(%d) NOT NULL,
                    %s,
                    INDEX span_key (span, pk_hash));
            """;

    private static final String INSERT_TABLE = """
                INSERT INTO %s.%s
                   (compare_sign, pk_hash, span, %s)
                   SELECT
                   UNHEX(MD5(CONCAT_WS('/', %s))),
                   UNHEX(MD5(CONCAT_WS('/', %s))),
                   UNHEX(LEFT(MD5(CONCAT_WS('/', %s)), %d)),
                   %s
                   FROM %s.%s;
            """;

    private static final String SUM_TABLE = """
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

    private static final String COMPARE_TABLE_DROP = """
                DROP TABLE %s.%s;
            """;

    private static final String RE_EMPTY_ALTER_TABLE = "^ALTER TABLE {0};$";

    private static final String COMPARE_TABLE_NAME = "compare_%s";

    public static void serverConnect(String server1Val, String server2Val, String db1, String db2, Map<String, Object> options) {
    }


    public static List<List<String>> getCommonObjects(Connection conn1, Connection conn2, boolean printList, Map<String, Object> options) throws SQLException {

        if (Objects.isNull(options)) options = new HashMap<>();

        List<String> db1Objects = getObjects(conn1, conn1.getCatalog(), options);
        List<String> db2Objects = getObjects(conn2, conn2.getCatalog(), options);

        //    in_both, in_db1_not_db2, in_db2_not_db1 = get_common_lists(db1_objects, db2_objects)
        Set<String> s1 = new HashSet<>(db1Objects);
        Set<String> s2 = new HashSet<>(db2Objects);
        Set<String> both = new HashSet<>(s1);

        both.retainAll(s2);
        s2.removeAll(both);
        s1.removeAll(both);

        List<String> inBoth = new ArrayList<>(both);
        List<String> inDb1NotDb2 = new ArrayList<>(s1);
        List<String> inDb2NotDb1 = new ArrayList<>(s2);

        Collections.sort(inBoth);

        if (printList) {
            String server1Str = "server1." + conn1.getCatalog();
            String server2Str;
            if (conn1.equals(conn2)) {
                server2Str = "server1." + conn2.getCatalog();
            } else {
                server2Str = "server2." + conn2.getCatalog();
            }

            printMissingList(inDb1NotDb2, server1Str, server2Str);
            printMissingList(inDb2NotDb1, server2Str, server1Str);
        }
        List<List<String>> result = new ArrayList<>();
        result.add(inBoth);
        result.add(inDb1NotDb2);
        result.add(inDb2NotDb1);

        return result;
    }

    public static boolean printMissingList(List<String> itemList, String first, String second) {

        if (itemList.size() == 0) {
            return false;
        }
        System.out.printf("# WARNING: Objects in %s but not in %s:", first, second);
        for (String item : itemList) {
            String[] parts = item.split(":");
            String type = parts[0];
            String name = parts.length > 1 ? parts[1] : "unknown";

            System.out.printf("# %12s: %s", name, type);
        }
        return true;
    }


    public static List<String> diffObjects(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Map<String, Object> options, String objType) throws SQLException {

        // options可以设置成一个具体的类，因为可设置的参数是固定的
        boolean quiet = (boolean) options.getOrDefault("quiet", false);
        String diffType = (String) options.getOrDefault("diffType", "unified");
        int width = (int) options.getOrDefault("width", 75);
        String direction = (String) options.getOrDefault("changes-for", "");
        boolean reverse = (boolean) options.getOrDefault("reverse", false);
        boolean skipTableOpts = (boolean) options.getOrDefault("skip_table_opts", false);
        boolean compactDiff = (boolean) options.getOrDefault("compact", false);

        String object1Create = getCreateObject(db1Conn, obj1, options, objType);
        String object2Create = getCreateObject(db2Conn, obj2, options, objType);

        if (objType.equals("DATABASE") && obj1.equals(obj2)) {
            String[] quotes = {"'", "\"", "`"};

            // 从数据库名中移除引号字符
            String db1 = removeQuotes(obj1, quotes);
            String db2 = removeQuotes(obj2, quotes);

            // 从创建语句中移除数据库名，并跳过第一个字符
            String first = removeDbNameAndSkipFirstChar(object1Create, db1);
            String second = removeDbNameAndSkipFirstChar(object2Create, db2);

            // 如果去除数据库名后的内容相同，则将两个创建语句置为空
            if (first.equals(second)) {
                object1Create = "";
                object2Create = "";
            }
        }

        if (!quiet) {
            System.out.printf("# Comparing %s to %s%n", obj1, obj2);
            //后面有输出空格，这里忽略了
        }

        List<String> object1CreateList = Arrays.stream(object1Create.split("\n")).toList();
        List<String> object2CreateList = Arrays.stream(object2Create.split("\n")).toList();


        List<String> diffServer1 = new ArrayList<>();
        List<String> diffServer2 = new ArrayList<>();
        List<String> transformServer1 = new ArrayList<>();
        List<String> transformServer2 = new ArrayList<>();


        // 支持选择方向或反转当前方向,这里的server1代表以server2为生产库，server1是容灾库
        if (direction.equals("server1") || direction.isEmpty() || reverse) {
            diffServer1 = getDiff(object1CreateList, object2CreateList, obj1, obj2, diffType, true);

            if (diffType.equals("sql") && diffServer1 != null) {
                transformServer1 = getTransform(db1Conn, db2Conn,
                        obj1, obj2, options,
                        objType);
            }
        }

        if (direction.equals("server2") || reverse) {
            diffServer2 = getDiff(object2CreateList, object1CreateList, obj2, obj1, diffType, compactDiff);

            if (diffType.equals("sql") && diffServer2 != null) {
                transformServer2 = getTransform(db2Conn, db1Conn,
                        obj2, obj1, options,
                        objType);
            }
        }
        // 用于提示用户的差异语句
        List<String> diffList = new ArrayList<>();

        if (direction.equals("server1") || direction.isEmpty()) {
            diffList = buildDiffList(diffServer1, diffServer2, transformServer1, transformServer2,
                    "server1", "server2", options);
        } else {
            diffList = buildDiffList(diffServer2, diffServer1, transformServer2, transformServer1,
                    "server2", "server1", options);
        }

        // 三个变量分别对应same_table_def， tbl_opts_diff， same_part_def
        ImmutableTriple<Boolean, List<String>, Boolean> structureCheck = new ImmutableTriple<>(false, null, false);

        // 这是完整的表定义
        Definition diffDefinition = new Definition();

        if (objType.equals("TABLE")) {
            structureCheck = checkTablesStructure(db1Conn, db2Conn, obj1, obj2, options, diffType);
        }

        //获取server1 的sql_mode
        String sqlMode = "";
        try(Statement stmt = db1Conn.createStatement()){
            ResultSet rs = stmt.executeQuery("SELECT @@SESSION.sql_mode");
            if (rs.next()) {
                sqlMode += rs.getString(1);
            }
        }

        // todo ansi_quotes以及后续的无效操作查询
        String regex_pattern = "";
        if (sqlMode.contains("ANSI_QUOTES")) {
            regex_pattern = String.format("r'{0}(?:(?:\\.){0})?'","r'(\"(?:[^\"]|\"\")+\"|\\w+|\\*)'");
        }
        else {
            regex_pattern = String.format(RE_EMPTY_ALTER_TABLE,String.format("r'{0}(?:(?:\\.){0})?'","r'(`(?:[^`]|``)+`|\\w+|\\w+[\\%\\*]?|[\\%\\*])'"));
        }

        if (!diffList.isEmpty() && structureCheck.getLeft() && structureCheck.getRight()) {
            System.out.println("[PASS]");
            return null;
        }

        if (!diffList.isEmpty() && direction.isEmpty() && structureCheck.getLeft()
                && structureCheck.getMiddle().isEmpty()) {
            if (!quiet) {
                System.out.println("[PASS]");
                System.out.println("# WARNING: The tables structure is the same, but the " +
                        "columns order is different. Use --change-for to take the " +
                        "order into account.");
            }
            return null;
        }

        if (diffType.equals("SQL") &&
                ((direction.equals("server1") && transformServer1.isEmpty() && !diffServer1.isEmpty()) ||
                        (direction.equals("server2") && transformServer2.isEmpty() && !diffServer2.isEmpty()))) {

            if (!quiet) System.out.println("[FAIL]");
            for (String line : diffList) {
                System.out.println(line);
            }
            System.out.printf("# WARNING: Could not generate SQL statements for differences " +
                    "between %s and %s. No changes required or not supported " +
                    "difference.%n", obj1, obj2);
            return diffList;
        }

        if (!diffList.isEmpty()) {
            if (!quiet) {
                System.out.println("[FAIL]");
            }

            if (!quiet || (!(boolean) options.getOrDefault("suppress_sql", false) && diffType.equals("sql"))) {
                for (String line : diffList) {
                    System.out.println(line);
                }

                if (diffDefinition.getPartDef()== null || diffDefinition.getPartDef().isEmpty()) {
                    System.out.println("# WARNING: Partition changes were not generated (not supported).");
                }
            }
            return diffList;
        }

        if (!quiet) {
            System.out.println("[PASS]");
            if (skipTableOpts && diffDefinition.getBasicDef().length > 0) {
                System.out.println("# WARNING: Table options are ignored and differences were found:");

                for (String diff : diffDefinition.getBasicDef()) {
                    System.out.printf("# %s", diff);
                }
            }
        }

        return null;
    }

    private static ImmutableTriple<Boolean, List<String>, Boolean> checkTablesStructure(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Map<String, Object> options, String diffType) {

        boolean compactDiff = (boolean) options.getOrDefault("compact", false);

        // 这是完整的表定义,模拟获取两个table对象
        Definition diffDefinition1 = new Definition(1);
        Definition diffDefinition2 = new Definition(2);

        // 获取表选项
        // 貌似不复杂，show create table的最后一行split空格分割
        List<String> table1Opts = diffDefinition1.getTableOptions();
        List<String> table2Opts = diffDefinition2.getTableOptions();
        List<String> diff = getDiff(table1Opts,table2Opts,obj1,obj2,diffType,compactDiff);

        // 检查表定义
        List<List<String>> table1Cols = diffDefinition1.getColDef();
        List<List<String>> table2Cols = diffDefinition2.getColDef();
        boolean sameColsDef = new HashSet(table1Cols).equals(new HashSet(table2Cols));

        // 检查表分区
        List<List<String>> table1Part = diffDefinition1.getPartDef();
        List<List<String>> table2Part = diffDefinition2.getPartDef();
        boolean samePartitionOpts = new HashSet(table1Part).equals(new HashSet(table2Part));


        return new ImmutableTriple<>(sameColsDef, diff, samePartitionOpts);
    }

    public static List<String> buildDiffList(List<String> diff1, List<String> diff2, List<String> transform1, List<String> transform2, String first, String second, Map<String, Object> options) {

        if(diff1.isEmpty()) return new ArrayList<>();

        boolean reverse = (boolean) options.getOrDefault("reverse", false);
        List<String> diffList = new ArrayList<>();
        String diffType = (String) options.getOrDefault("diff_type", "sql");
        if(diffType.equals("sql")) {
            if(transform1.isEmpty()) {
                diffList.add("\n# WARNING: Cannot generate SQL statements for these objects.");
                diffList.add("# Check the difference output for other discrepencies.");
                diffList.addAll(diff1);
            }
            else {
                diffList.add(String.format("# Transformation for --changes-for=%s:\n#\n",first));
                diffList.addAll(transform1);
                diffList.add("");
                if (reverse && !(transform2.isEmpty())) {
                    diffList.add(String.format("#\n# Transformation for reverse changes (--changes-for=%s):\n#" ,second));
                    for (String row : transform2) {
                        String[] subRows = row.split("\n");
                        for (String subRow : subRows) {
                            diffList.add(String.format("# %s", subRow));
                        }

                    }
                    diffList.add("#\n");
                }
            }
        }
        else {
            if(!(boolean) options.getOrDefault("data_diff",false)) {
                diffList.add(String.format("# Object definitions differ. (--changes-for=%s)\n#\n",first));
            }
            diffList.addAll(diff1);

            if(reverse && !diff2.isEmpty()) {
                diffList.add("");
                if(!(boolean) options.getOrDefault("data_diff",false)){
                    diffList.add(String.format("#\n# Definition diff for reverse changes " +
                            "(--changes-for=%s):\n#", second));
                }
                for(String row : diff2){
                    diffList.add(String.format("# %s", row));
                }
                diffList.add("#\n");
            }
        }

        return diffList;
    }

    private static List<String> getTransform(Connection db1Conn, Connection db2Conn, String name1, String name2, Map<String, Object> options, String objType) throws SQLException {

        if (name1.isEmpty() || objType.equals("database")) {
            name1 = db1Conn.getCatalog();
            name1 = db2Conn.getCatalog();
        }

        Definition obj1 = getObjectDefinition(db1Conn.getCatalog(), name1, objType, 1);
        Definition obj2 = getObjectDefinition(db2Conn.getCatalog(), name2, objType, 2);

        List<String> transformStr = new ArrayList<>();

        SQLTransformer xform = new SQLTransformer(db1Conn.getCatalog(), db2Conn.getCatalog(),
                obj1, obj2, objType, (int) options.getOrDefault("verbosity", 0), options);
        transformStr = xform.transformDefinition();

        return transformStr;
    }

    /**
     * 获取对象定义(这个参数num没有意义，只是区分模拟的对象)
     *
     * @return String[] basicDef;
     * List<String[]> colDef;
     * List<String[]> partDef;
     */
    private static Definition getObjectDefinition(String catalog, String name1, String objType, int num) {

        // 这里有一些通过INFORMATION_SCHEMA获取的数据以及数据处理工作，我直接模拟一个可能的结果

        return new Definition(num);
    }

    /**
     * 得到行差异
     * @return List(String)
     */
    private static List<String> getDiff(List<String> object1CreateList, List<String> object2CreateList, String obj1, String obj2, String diffType, boolean compactDiff) {

        List<String> diffStr = new ArrayList<>();
        Patch<String> patch = DiffUtils.diff(object1CreateList, object2CreateList);

        if (Set.of("unified", "sql").contains(diffType)) {
            List<String> unifiedDiff = UnifiedDiffUtils.generateUnifiedDiff(obj1, obj2, object1CreateList, patch, 0);

            for (String line : unifiedDiff) {
                if (compactDiff) {
                    if (line.startsWith("@@ ")) {
                        diffStr.add(line.strip());
                    } else {
                        diffStr.add(line.strip());
                    }
                }
            }
        }
        // 这个工具只支持unified，后面的先放一下
        else if (diffType.equals("context")) {
            List<String> unifiedDiff = UnifiedDiffUtils.generateUnifiedDiff(obj1, obj2, object1CreateList, patch, 0);
        }


        return diffStr;
    }

    /**
     * 移除引号
     */
    private static String removeQuotes(String input, String[] quotes) {
        String result = input;
        for (String quote : quotes) {
            result = result.replace(quote, "");
        }
        return result;
    }

    /**
     * 从创建语句中移除数据库名并跳过第一个字符
     */
    private static String removeDbNameAndSkipFirstChar(String createStmt, String dbName) {
        // 移除数据库名
        String withoutDbName = createStmt.replace(dbName, "");
        // 跳过第一个字符（[1::] 的效果）
        if (withoutDbName.length() > 0) {
            return withoutDbName.substring(1);
        }
        return withoutDbName;
    }

    /**
     * get_create_object
     */
    public static String getCreateObject(Connection dbConn, String obj, Map<String, Object> options, String objType) throws SQLException {

        int verbosity = (int) options.getOrDefault("verbosity", 0);
        boolean quiet = (boolean) options.getOrDefault("quiet", false);

        // 原代码是当type是database时， 将type和name都设置成database
        if (obj.isEmpty() || objType.equals("DATABASE")) {
            obj = dbConn.getCatalog();
        }

        // 这里是db.get_create_statement
        String showCreateSql = String.format("SHOW CREATE %s %s", objType, obj);

        try (Statement statement = dbConn.createStatement();
             ResultSet rs = statement.executeQuery(showCreateSql);) {

            String createStmt = "";
            if (objType.equals("TABLE") || objType.equals("VIEW") || objType.equals("DATABASE")) {
                // create table语句在第二位
                if (rs.next()) createStmt = rs.getString(2);
            } else {
                if (rs.next()) createStmt = rs.getString(3);
            }

            if (verbosity > 0 && !quiet) {
                if (!obj.isEmpty()) {
                    System.out.printf("\n# Definition for object %s.%s:%n", dbConn.getCatalog(), obj);
                } else {
                    System.out.printf("\n# Definition for object %s:%n", dbConn.getCatalog());
                }

                System.out.println(createStmt);
            }
            return createStmt;

        }
    }

    /**
     * 原方法返回的是三个列表，直接在主函数中处理
     */
    public static void getCommonList() {
    }

    /**
     * 拼接table_name 和 table_type
     * @return table_name:table_type
     */
    public static List<String> getObjects(Connection connection, String databaseName, Map<String, Object> options) throws SQLException {
        List<String> objects = new ArrayList<>();

        // 这里的options完全没用上
        options.replace("skip_grants", Boolean.TRUE);

        // 获取表
        DatabaseMetaData metaData = connection.getMetaData();
        try(ResultSet tables = metaData.getTables(databaseName, null, null,
                new String[]{"TABLE", "VIEW"});) {
            while (tables.next()) {
                objects.add(tables.getString("TABLE_TYPE") + ":" + tables.getString("TABLE_NAME"));
            }
        }

        try(ResultSet functions = metaData.getFunctions(databaseName, null, null);) {
            while (functions.next()) {
                objects.add("FUNCTION:"+functions.getString("FUNCTION_NAME"));
            }
        }

        try(ResultSet procedure = metaData.getProcedures(databaseName, null, null);) {
            while (procedure.next()) {
                String name = procedure.getString("PROCEDURE_NAME");
                boolean found = false;
                for(String obj :objects){
                    if(obj.contains(name)) {
                        found = true;
                        break;
                    }

                }
                if(!found) objects.add("PROCEDURE:"+name);
            }
        }

//        List<String> objs = Arrays.asList("EVENT:evt_daily_cleanup",
//                "FUNCTION:calculate_total",
//                "FUNCTION:format_date",
//                "PROCEDURE:cleanup_old_data",
//                "PROCEDURE:update_stats",
//                "TABLE:role",
//                "TABLE:user",
//                "TRIGGER:trg_orders_insert",
//                "VIEW:customer_summary",
//                "TABLE:orders");

        // 对结果进行排序
        Collections.sort(objects);

        return objects;
    }

    public static DiffServer checkConsistency(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Map<String, Object> options, DbCompareEntry.Reporter reporter) throws SQLException {
        if (options == null) {
            options = new HashMap<>();
        }
        int spanKeySize = (int) options.getOrDefault("span_key_size", DEFAULT_SPAN_KEY_SIZE);
        List<String> useIndexes = (List<String>) options.getOrDefault("use_indexes", null);
        String direction = (String) options.getOrDefault("changes-for", "server1");
        boolean reverse = (boolean) options.getOrDefault("reverse", false);

        if (reporter != null) {
            reporter.reportObject("", "- Compare table checksum");
            reporter.reportState("");
            reporter.reportState("");
        }
        if (Objects.isNull(options.get("no_checksum_table")) || !(boolean) options.get("no_checksum_table")) {
            Long checksum1 = CheckSum(db1Conn, obj1);
            Long checksum2 = CheckSum(db2Conn, obj2);

            // 这里没有err，就用checksum1 == 0来判断了
            if (checksum1 == 0 || checksum2 == 0) {
                throw new SQLException(String.format("Error executing CHECKSUM TABLE on %s:%s or %s:%s",
                        db1Conn.getCatalog(), obj1, db2Conn.getCatalog(), obj2));
            }

            if (checksum1.equals(checksum2)) {
                if (reporter != null) {
                    reporter.reportState("pass");
                }
                return null;
            } else {
                if (reporter != null) {
                    reporter.reportState("FAIL");
                }
            }
        } else if (reporter != null) {
            reporter.reportState("SKIP");
        }

        // todo 通过用户设置的index进行后续的数据一致性检验操作
        //unq_use_indexes, table1_use_indexes, table2_use_indexes

        // 检查二进制日志状态
        boolean binlogServer1 = false;
        boolean binlogServer2 = false;
        if ((boolean) options.getOrDefault("toggle_binlog", false)) {
            binlogServer1 = isBinlogEnabled(db1Conn);
            binlogServer2 = isBinlogEnabled(db2Conn);
            if (binlogServer1) {
                db1Conn.rollback();
                disableBinlog(db1Conn);
            }
            if (binlogServer2) {
                db2Conn.commit();
                disableBinlog(db2Conn);
            }
        }

        List<String> dataDiffs1 = new ArrayList<>();
        List<String> dataDiffs2 = new ArrayList<>();

        if (reporter != null) {
            reporter.reportObject("", "- Find row differences");
            reporter.reportState("");
            reporter.reportState("");
        }

        // 去除前面的database
        obj1 = obj1.split("\\.")[1];
        obj2 = obj2.split("\\.")[1];
        // 这里还应该有个使用 用户输入的index的操作
        setupCompare(db1Conn, db2Conn, obj1, obj2, spanKeySize, useIndexes);

        String compareTblName = "compare_" + obj1;

        // 填充比较表，并从每个表中检索行
        List<String[]> tbl1Hash = makeSumRows(db1Conn, db1Conn.getCatalog(), compareTblName, obj1);
        List<String[]> tbl2Hash = makeSumRows(db2Conn, db2Conn.getCatalog(), compareTblName, obj2);

        // 计算交集，类似getCommonList
        Set<String[]> in1Not2 = new HashSet<>(tbl1Hash);
        Set<String[]> in2Not1 = new HashSet<>(tbl2Hash);
        Set<String[]> same = new HashSet<>(in1Not2);
        same.retainAll(in2Not1);

        // 计算仅在set1中的元素
        in1Not2.removeAll(same);

        // 计算仅在set2中的元素
        in2Not1.removeAll(same);

        if (!in1Not2.isEmpty() || !in2Not1.isEmpty()) {
            List<String> tableDiffs1 = new ArrayList<>();
            List<String> tableDiffs2 = new ArrayList<>();
            for (String[] str : in1Not2) {
                tableDiffs1.add(str[0]);
            }

            for (String[] str : in2Not1) {
                tableDiffs2.add(str[0]);
            }

            Set<String> changedRows = new HashSet<>(tableDiffs1);
            Set<String> extra2 = new HashSet<>(tableDiffs2);
            Set<String> extra1 = new HashSet<>(tableDiffs1);

            changedRows.retainAll(extra2);
            extra1.removeAll(changedRows);
            extra2.removeAll(changedRows);

            db1Conn.setAutoCommit(false);
            db2Conn.setAutoCommit(false);

            // 这里是后续的整个_generate_data_diff_output
            if (direction.equals("server1") || reverse) {
                dataDiffs1 = generateDataDiffOutput(new ImmutableTriple<>(changedRows, extra1, extra2), db1Conn, db2Conn, obj1, obj2, useIndexes, options);
            }

            if (direction.equals("server2") || reverse) {
                dataDiffs2 = generateDataDiffOutput(new ImmutableTriple<>(changedRows, extra2, extra1), db2Conn, db1Conn, obj2, obj1, useIndexes, options);
            }

        }
        if (binlogServer1) {
            db1Conn.commit();
            db1Conn.setAutoCommit(true);
        }
        if (binlogServer2) {
            db2Conn.commit();
            db1Conn.setAutoCommit(true);
        }

        if (reporter != null) {
            if (!dataDiffs1.isEmpty() || !dataDiffs2.isEmpty()) {
                reporter.reportState("FAIL");
            } else {
                reporter.reportState("pass");
            }
        }

        DiffServer diffServer = new DiffServer();
        if (direction.equals("server1") || reverse) {
            diffServer.setFirst(dataDiffs1.isEmpty() ? null : dataDiffs1);
        }
        if (direction.equals("server2") || reverse) {
            diffServer.setSecond(dataDiffs2.isEmpty() ? null : dataDiffs2);
        }

        return diffServer;
    }

    private static List<String> generateDataDiffOutput(
            ImmutableTriple<Set<String>, Set<String>, Set<String>> immutableTriple,
            Connection dbConn1, Connection dbConn2,
            String obj1, String obj2, List<String> useIndexes, Map<String, Object> options) throws SQLException {

        String diffType = (String) options.getOrDefault("difftype", "unified");
        String fmt = (String) options.getOrDefault("format", "grid");
        boolean compactDiff = (boolean) options.getOrDefault("compact", false);

        Set<String> changedRows = immutableTriple.getLeft();
        Set<String> extra1 = immutableTriple.getMiddle();
        Set<String> extra2 = immutableTriple.getRight();

        List<String> dataDiffs = new ArrayList<>();

        ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>> tbl1Rows = null;
        ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>> tbl2Rows = null;


        List<Map<String, Object>> changedIn1 = new ArrayList<>();
        List<Map<String, Object>> changedIn2 = new ArrayList<>();
        List<Map<String, Object>> extraIn1 = new ArrayList<>();
        List<Map<String, Object>> extraIn2 = new ArrayList<>();

        if (changedRows.size() > 0) {

            dataDiffs.add("# Data differences found among rows:");

            ImmutablePair<ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>>, ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>>>
                    tblRow = getChangedRowsSpan(dbConn1, obj1, dbConn2, obj2, changedRows, useIndexes);

            tbl1Rows = tblRow.getLeft();
            tbl2Rows = tblRow.getRight();

        }

        if (!Objects.isNull(tbl1Rows)) {
            changedIn1.addAll(tbl1Rows.getLeft());
            extraIn1.addAll(tbl1Rows.getRight());
        }
        if (!Objects.isNull(tbl2Rows)) {
            changedIn2.addAll(tbl2Rows.getLeft());
            extraIn2.addAll(tbl2Rows.getRight());
        }

        if (extra1.size() > 0) {
            List<Map<String, Object>> resultList = getRowSpan(obj1, extra1, dbConn1);
            extraIn1.addAll(resultList);
        }

        if (extra2.size() > 0) {
            List<Map<String, Object>> resultList = getRowSpan(obj2, extra2, dbConn2);
            extraIn2.addAll(resultList);
        }

        // 如果changedIn1不为空 表示需要 update table2 ，extraIn1 需要table2 insert， extraIn2 需要table2 delete
        if (!changedIn1.isEmpty() || !changedIn2.isEmpty() || !extraIn1.isEmpty() || !extraIn2.isEmpty()) {
            List<String> fixSql = generateFixSql(changedIn1, extraIn1, extraIn2, obj2);

            dataDiffs.addAll(fixSql);
//            for (String sql : fixSql) {
//                System.out.println(sql);
//            }
        }

        return dataDiffs;
    }

    private static ImmutablePair<ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>>, ImmutablePair<List<Map<String, Object>>, List<Map<String, Object>>>> getChangedRowsSpan(
            Connection db1Conn, String obj1, Connection db2Conn, String obj2, Set<String> changedRows, List<String> useIndexes) throws SQLException {

        List<SpanData> fullSpanData1 = new ArrayList<>();
        List<SpanData> fullSpanData2 = new ArrayList<>();

        String qTblName1 = "compare_" + obj1;
        String qTblName2 = "compare_" + obj2;
        for (String str : changedRows) {
            try (PreparedStatement statement = db1Conn.prepareStatement(
                    String.format(DIFF_COMPARE, db1Conn.getCatalog(), qTblName1, str));) {
                statement.setFetchSize(100);
                statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                try (ResultSet resultSet = statement.executeQuery();) {
                    List<String[]> spanRowList = new ArrayList<>();
                    Set<RowSignature> cmpSigns = new HashSet<>();
                    while (resultSet.next()) {
                        String[] spanRow = new String[3 + PRIMARY_KEYS.size()];
                        for (int i = 1; i <= 3 + PRIMARY_KEYS.size(); i++) {
                            spanRow[i - 1] = resultSet.getString(i);
                        }
                        spanRowList.add(spanRow);
                        RowSignature signature = new RowSignature(
                                resultSet.getString(1),  // compare_sign
                                resultSet.getString(2)   // pk_hash
                        );
                        cmpSigns.add(signature);
                    }
                    fullSpanData1.add(new SpanData(spanRowList, cmpSigns));
                }
            }
        }


        for (String str : changedRows) {
            try (PreparedStatement statement = db2Conn.prepareStatement(String.format(DIFF_COMPARE, db2Conn.getCatalog(), qTblName2, str));) {
                statement.setFetchSize(100);
                statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                try (ResultSet resultSet = statement.executeQuery();) {
                    List<String[]> spanRowList = new ArrayList<>();
                    Set<RowSignature> cmpSigns = new HashSet<>();
                    while (resultSet.next()) {
                        String[] spanRow = new String[3 + PRIMARY_KEYS.size()];

                        for (int i = 1; i <= 3 + PRIMARY_KEYS.size(); i++) {
                            spanRow[i - 1] = resultSet.getString(i);
                        }
                        spanRowList.add(spanRow);
                        RowSignature signature = new RowSignature(
                                resultSet.getString(1),  // compare_sign
                                resultSet.getString(2)   // pk_hash
                        );
                        cmpSigns.add(signature);
                    }
                    fullSpanData2.add(new SpanData(spanRowList, cmpSigns));
                }
            }
        }


        List<Map<String, Object>> changedIn1 = new ArrayList<>();
        List<Map<String, Object>> changedIn2 = new ArrayList<>();
        List<Map<String, Object>> extraIn1 = new ArrayList<>();
        List<Map<String, Object>> extraIn2 = new ArrayList<>();

        for (int pos = 0; pos < fullSpanData1.size(); pos++) {
            SpanData spanData1 = fullSpanData1.get(pos);
            SpanData spanData2 = fullSpanData2.get(pos);

            // 确定表1和表2的不同行（排除未更改的行）
            Set<RowSignature> diffRowsSign1 = calculateDifference(spanData1.getSignatures(), spanData2.getSignatures());
            Set<RowSignature> diffRowsSign2 = calculateDifference(spanData2.getSignatures(), spanData1.getSignatures());

            // 提取比较签名中的pk_hash
            Set<String> diffPkHash1 = extractPkHashes(diffRowsSign1);
            Set<String> diffPkHash2 = extractPkHashes(diffRowsSign2);


            for (String[] res : spanData1.getRowData()) {
                if (diffRowsSign1.contains(new RowSignature(res[0], res[1]))) {
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
                            db1Conn.getCatalog(),
                            obj1,
                            whereClauseStr);

                    try (PreparedStatement Statement = db1Conn.prepareStatement(query);
                         ResultSet needChangeRes = Statement.executeQuery()) {
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

            for (String[] res : spanData2.getRowData()) {
                if (diffRowsSign2.contains(new RowSignature(res[0], res[1]))) {
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
                            db2Conn.getCatalog(),
                            obj2,
                            whereClauseStr);

                    try (PreparedStatement Statement = db1Conn.prepareStatement(query);
                         ResultSet needChangeRes = Statement.executeQuery();) {
                        // 处理查询结果并分类
                        if (needChangeRes.next()) {
                            Map<String, Object> rowData = convertResultSetToMap(needChangeRes);

                            if (diffPkHash1.contains(res[1])) {
                                // 存储原始行（需要DELETE）
                                changedIn2.add(rowData);
                            } else {
                                // 存储原始行（需要DELETE）
                                extraIn2.add(rowData);
                            }
                        }
                    }
                }
            }

        }

        return new ImmutablePair<>(new ImmutablePair<>(changedIn1, extraIn1), new ImmutablePair<>(changedIn2, extraIn2));
    }

    /**
     * 根据变更数据生成SQL修复语句
     * @param changedIn1 需要更新的变更行（表1到表2）
     * @param extraIn1   需要删除的额外行（表1独有）
     * @param extraIn2   需要插入的额外行（表2独有）
     * @param table2     容灾表名
     * @return SQL修复语句列表
     */
    private static List<String> generateFixSql(List<Map<String, Object>> changedIn1,
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
    private static String generateUpdateSql(String tableName, Map<String, Object> rowData) {
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
    private static String generateDeleteSql(String tableName, Map<String, Object> rowData) {
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
    private static String generateInsertSql(String tableName, Map<String, Object> rowData) {
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
    private static void buildWhereClauseFromPrimaryKeys(Map<String, Object> rowData, StringBuilder whereClause) {
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


    private static List<Map<String, Object>> getRowSpan(String tableName, Set<String> extraSpans, Connection conn) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<String[]> allPkValues = new ArrayList<>();

        try {
            String cateLog = conn.getCatalog();
            String compareTable = "compare_" + tableName;
            // 收集所有主键值
            for (String span : extraSpans) {
                String diffQuery = String.format(DIFF_COMPARE, cateLog, compareTable, span);
                try (PreparedStatement statement = conn.prepareStatement(diffQuery);
                     ResultSet spanResultSet = statement.executeQuery();) {
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

                try (PreparedStatement statement = conn.prepareStatement(originalQuery);
                     ResultSet originalResultSet = statement.executeQuery();) {
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

    // 将结果转成map
    private static Map<String, Object> convertResultSetToMap(ResultSet rs) throws SQLException {
        Map<String, Object> rowData = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            rowData.put(metaData.getColumnName(i), rs.getObject(i));
        }

        return rowData;
    }

    // 可有可无
    private static Set<RowSignature> calculateDifference(Set<RowSignature> set1, Set<RowSignature> set2) {
        Set<RowSignature> difference = new HashSet<>(set1);
        difference.removeAll(set2);
        return difference;
    }

    private static Set<String> extractPkHashes(Set<RowSignature> diffRows) {
        Set<String> pkHashes = new HashSet<>();
        for (RowSignature signature : diffRows) {
            pkHashes.add(signature.getPkHash());
        }
        return pkHashes;
    }

    private static List<String[]> makeSumRows(Connection conn, String dbName, String compareTblName, String tableName) throws SQLException {

        String pkStr = String.join(",", PRIMARY_KEYS);

        COMPARE_COLUMNS.clear();
        // 拿列名
        String sql1 = "select * from %s limit 1".formatted(tableName);
        try(PreparedStatement statement = conn.prepareStatement(sql1);
                ResultSet resultSet = statement.executeQuery(sql1);){
            //拿列名
            ResultSetMetaData metaData = resultSet.getMetaData();
            //获取列集
            int columnCount = metaData.getColumnCount(); //获取列的数量
            for(int i = 1; i <= columnCount; i++){
                COMPARE_COLUMNS.add(i-1,metaData.getColumnName(i));
            }
        }

        String colStr = String.join(",", COMPARE_COLUMNS);

        String tempSql = String.format(INSERT_TABLE,
                dbName,
                compareTblName,
                pkStr,
                colStr,
                pkStr,
                pkStr,
                DEFAULT_SPAN_KEY_SIZE,
                pkStr,
                dbName,
                tableName);

        try (PreparedStatement statement = conn.prepareStatement(tempSql);) {
            statement.execute();
        }

        tempSql = String.format(SUM_TABLE, dbName, compareTblName);

        try (PreparedStatement statement = conn.prepareStatement(tempSql)) {
            statement.setFetchSize(1000);

            try (ResultSet resultSet = statement.executeQuery()) {
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


    // 这里不还原，python的table对象可以反找server对象，我这里需要传连接对象
    private static void setupCompare(Connection conn1, Connection conn2, String table1, String table2, int spanKeySize, List<String> useIndexes) throws SQLException {
        PRIMARY_KEYS.clear();
        List<String> diagMsgs = new ArrayList<>();
        DatabaseMetaData dbMetaData1 = conn1.getMetaData();
        DatabaseMetaData dbMetaData2 = conn2.getMetaData();
        // 拿两张表的主键
        try (ResultSet set1 = dbMetaData1.getPrimaryKeys(conn1.getCatalog(), conn1.getSchema(), table1);
             ResultSet set2 = dbMetaData2.getPrimaryKeys(conn2.getCatalog(), conn2.getSchema(), table2)) {
            while (set1.next()) {
                String pk1 = set1.getString("COLUMN_NAME");

                PRIMARY_KEYS.add(pk1);
                // 两张表主键不相同
                if (set2.next()) {
                    if (!set1.getString("COLUMN_NAME").equals(pk1)) {
                        throw new RuntimeException(String.format("The specified index %s was not found in table %s", table1, table2));
                    }
                } else {
                    throw new RuntimeException(String.format("The specified index %s was not found in table %s", table1, table2));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Indexes are not the same" + e);
        }

        // 丢弃旧的临时表
        dropCompareObject(conn1, table1);
        dropCompareObject(conn2, table2);

        // 获取创表语句
        String tbl1Table = String.format(COMPARE_TABLE,
                conn1.getCatalog(),
                String.format(COMPARE_TABLE_NAME, table1),
                spanKeySize / 2,
                buildIndexDefinition());

        String tbl2Table = String.format(COMPARE_TABLE,
                conn2.getCatalog(),
                String.format(COMPARE_TABLE_NAME, table2),
                spanKeySize / 2,
                buildIndexDefinition());


        boolean mustToggle1 = !conn1.getAutoCommit();
        if (mustToggle1) {
            conn1.setAutoCommit(true);
        }
        boolean mustToggle2 = !conn2.getAutoCommit();
        if (mustToggle2) {
            conn2.setAutoCommit(true);
        }


        // 创建临时表
        try (PreparedStatement statement1 = conn1.prepareStatement(tbl1Table);
             PreparedStatement statement2 = conn2.prepareStatement(tbl2Table);
        ) {
            statement1.execute();
            statement2.execute();
        }

        if (mustToggle1) conn1.setAutoCommit(false);
        if (mustToggle2) conn2.setAutoCommit(false);

    }

    private static String buildIndexDefinition() {
        StringBuilder indexDefn = new StringBuilder();

        for (String primaryKey : PRIMARY_KEYS) {
            indexDefn.append(primaryKey)
                    .append(" VARCHAR(255), ");
        }

        // 移除最后的逗号和空格
        if (indexDefn.length() > 0) {
            indexDefn.setLength(indexDefn.length() - 2);
        }

        return indexDefn.toString();
    }

    private static void dropCompareObject(Connection conn, String table) {
        try {
            boolean toggleServer = !conn.getAutoCommit();
            if (toggleServer) {
                conn.setAutoCommit(true);
            }
            String tTable = String.format(COMPARE_TABLE_NAME, table);

            try (PreparedStatement statement = conn.prepareStatement(String.format(COMPARE_TABLE_DROP, conn.getCatalog(), tTable))) {
                statement.executeQuery();
            }

            if (toggleServer) {
                conn.setAutoCommit(false);
            }

        } catch (SQLException e) {
            return;
        }
    }


    private static Long CheckSum(Connection conn, String table) {
        String checkSql = "checksum table %s".formatted(table);
        long checksum = 0;

        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(checkSql);) {

            while (rs.next()) {
                checksum = rs.getLong("Checksum");
            }
            return checksum;
        } catch (SQLException e) {
            return 0L;
        }

    }

    public static boolean isBinlogEnabled(Connection conn) throws SQLException {
        String sql = "SHOW VARIABLES LIKE 'log_bin'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return "ON".equalsIgnoreCase(rs.getString("Value"));
            }
            return false;
        }
    }

    public static void disableBinlog(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // 提交当前事务以避免在事务中设置 sql_log_bin 的错误
            conn.rollback(); // 或 conn.commit()
            // 关闭二进制日志
            stmt.execute("SET sql_log_bin = 0");
        }
    }

    @Getter
    @Setter
    public static class DiffServer {
        private List<String> first;
        private List<String> second;
    }
}
