package cn.infocore.dbs.compare.verify;

import cn.infocore.dbs.compare.model.DbConnection;
import cn.infocore.dbs.compare.model.ObjectDiff;
import cn.infocore.dbs.compare.model.dto.DbResultDto;
import cn.infocore.dbs.compare.verify.compare.CheckResult;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

/**
 * 数据库校验入口类
 */
@Component
public class DbCompareEntry {

    public final static int DEFAULT_SPAN_KEY_SIZE = 8;

    private final static String ERROR_DB_DIFF = "The object definitions do not match.";
    private final static String ERROR_DB_MISSING = "The database %s does not exist.";
    private final static String ERROR_OBJECT_LIST = "The list of objects differs among database %s and %s.";
    private final static String ERROR_ORW_COUNT = "Row counts are not the same among %s and %s.\n#";
    private final static Map<String, Object> DEFAULT_OPTIONS = new HashMap<>() {{
        put("quiet", false);
        put("verbosity", 3);
        put("difftype", "differ");
        put("run_all_tests", true);
        put("width", 75);
        put("no_object_check", false);
        put("no_diff", false);
        put("no_row_count", false);
        put("no_data", false);
        put("transform", false);
        put("span_key_size", DEFAULT_SPAN_KEY_SIZE);
    }};
    private final static String ROW_FORMAT1 = "# %s %s %s %s %s";
    private final static int PRINT_WIDTH = 75;

    /**
     * @param server1Val 服务器1连接信息s
     * @param server2Val 服务器2连接信息
     * @param db1        数据库1
     * @param db2        数据库2
     * @param options    配置选项
     * @return 对比结果 - true表示全部匹配，false表示有差异
     */
    public DbResultDto databaseCompare(DbConnection server1Val, DbConnection server2Val, String db1, String db2, Map<String, Object> options, int test) throws SQLException {

        DbResultDto dbResultDto = new DbResultDto();

        if (Objects.isNull(options)) options = new HashMap<>();
        checkOptionDefault(options);
        boolean quiet = (boolean) options.getOrDefault("quiet", "False");

        // 提前声明函数结果success
        boolean success = false;

        // 得到两个数据库
        DbCompareUtils.serverConnect(server1Val.getHost(), server2Val.getHost(), db1, db2, options);

        String dbUrl = "jdbc:%s://%s:%d/%s";

        String url1 = String.format(dbUrl,server1Val.getDbType(),server1Val.getHost(),server1Val.getPort(),db1);
        String url2 = String.format(dbUrl,server2Val.getDbType(),server2Val.getHost(),server2Val.getPort(),db2);

        // 直接从conn开始吧
        try (Connection db1Conn = DriverManager.getConnection(url1, server1Val.getUsername(), server1Val.getPassword());
             Connection db2Conn = DriverManager.getConnection(url2, server2Val.getUsername(), server1Val.getPassword())) {

            // 以秒为单位
            if (!db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db1));
            if (!db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db2));

            if (!quiet) {
                String message;
                if (Objects.isNull(server2Val)) {
                    message = "# Checking databases %s and %s on server1\n#";
                } else {
                    message = ("# Checking databases %s on server1 and %s on server2\n#");
                }

                System.out.printf((message) + "%n", db1Conn.getCatalog(), db2Conn.getCatalog());
            }

            // 有问题的话到不了这一步
            // 这里比较的是对象的差异，貌似没需求，直接忽略
            //List<ObjectDiff> objectDiffList = checkDatabases(db1Conn, db2Conn, db1, db2, options);

            //_check_objects
            CheckResult checkResult = checkObjects(db1Conn, db2Conn, db1, db2, options);

            // 这里是python中第一次出现success
            success = !checkResult.isDiffers();

            List<String> inDb1 = checkResult.getInDb1();
            List<String> inDb2 = checkResult.getInDb2();

            List<ObjectDiff> objectDiffList = new ArrayList<>();
            for(String str: inDb1){
                objectDiffList.add(new ObjectDiff(str,false,false,"缺失表"+str));
            }
            for(String str: inDb2){
                objectDiffList.add(new ObjectDiff(str,false,false,"多余表"+str));
            }

            // reporter 用于打印输出，暂时忽略sqlMode
            Reporter reporter = new Reporter(options);
            reporter.printHeading();

            // inBoth格式：   table_type:table_name
            List<String> inBoth = checkResult.getInBoth();

            for (String item : inBoth) {
                List<String> errorList = new ArrayList<>();
                List<String> debugMsgs = new ArrayList<>();

                String objType = item.split(":")[0];

                String qObj1 = String.format("%s.%s", db1, item.split(":")[1]);
                String qObj2 = String.format("%s.%s", db2, item.split(":")[1]);

                List<String> errors = compareObject(db1Conn, db2Conn, qObj1, qObj2, reporter, options, objType);

                errorList.addAll(errors);

                if (objType.equals("TABLE")) {
                    errors = checkRowCounts(db1Conn, db2Conn, qObj1, qObj2, reporter, options);
                    if (!errors.isEmpty()) {
                        errorList.addAll(errors);
                    }
                } else {
                    reporter.reportState("-");
                }

                if (objType.equals("TABLE")) {
                    // 这里应该是由两个返回值的，但是貌似第二个返回值一直为空，且后续没有使用
                    errors = checkDataConsistency(db1Conn, db2Conn, qObj1, qObj2, reporter, options);
                    if (!errors.isEmpty()) {
                        errorList.addAll(errors);
                    }
                } else reporter.reportState("-");
                // selectedCode部分的逻辑
                if ((int) options.get("verbosity") > 0) {
                    if (!quiet) {
                        System.out.println();
                    }
                    try {
                        String createObj1 = DbCompareUtils.getCreateObject(db1Conn, qObj1, options, objType);
                        String createObj2 = DbCompareUtils.getCreateObject(db2Conn, qObj2, options, objType);

                    } catch (SQLException e) {
                        System.err.println("Error getting object definition: " + e.getMessage());
                    }
                }

                if (debugMsgs != null && !debugMsgs.isEmpty() && (int) options.get("verbosity") > 2) {
                    reporter.reportErrors(debugMsgs);
                }

                if (!quiet) {
                    reporter.reportErrors(errorList);
                }
                if (!errorList.isEmpty()) {
                    success = false;
                }

            }

            return null;
        }
    }

    private List<ObjectDiff> checkDatabases(Connection db1Conn, Connection db2Conn, String db1, String db2, Map<String, Object> options) throws SQLException {
        if(!(boolean) options.get("no_diff")){
            Map<String,Object> new_opt = new HashMap<>();
            new_opt.putAll(options);
            new_opt.put("quiet",true);
            new_opt.put("suppress_sql",true);

            List<String> res = DbCompareUtils.diffObjects(db1Conn, db2Conn, db1, db2, options, "DATABASE");
            if(!res.isEmpty()) {
                for (String row: res){
                    System.out.println(row);
                }
                System.out.println();
                if (!(boolean) options.get("run_all_tests") && !(boolean) options.getOrDefault("quiet", false)){
                    System.err.println(ERROR_DB_DIFF);
                }
                //return res;
            }
        }
        return null;
    }


    /**
     * @param server1Val 服务器1连接信息s
     * @param server2Val 服务器2连接信息
     * @param db1        数据库1
     * @param db2        数据库2
     * @param options    配置选项
     * @return 对比结果 - true表示全部匹配，false表示有差异
     */
    public boolean databaseCompare(DbConnection server1Val, DbConnection server2Val, String db1, String db2, Map<String, Object> options) throws SQLException {

        if (Objects.isNull(options)) options = new HashMap<>();
        checkOptionDefault(options);
        boolean quiet = (boolean) options.getOrDefault("quiet", "False");

        // 提前声明函数结果success
        boolean success = false;

        // 得到两个数据库
        DbCompareUtils.serverConnect(server1Val.getHost(), server2Val.getHost(), db1, db2, options);

        String dbUrl = "jdbc:%s://%s:%d";

        String url1 = String.format(dbUrl,server1Val.getDbType(),server1Val.getHost(),server1Val.getPort());
        String url2 = String.format(dbUrl,server2Val.getDbType(),server2Val.getHost(),server2Val.getPort());

        // 直接从conn开始吧
        try (Connection db1Conn = DriverManager.getConnection(url1, server1Val.getUsername(), server1Val.getPassword());
             Connection db2Conn = DriverManager.getConnection(url2, server2Val.getUsername(), server1Val.getPassword())) {

            // 以秒为单位
            if (!db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db1));
            if (!db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db2));

            if (!quiet) {
                String message;
                if (Objects.isNull(server2Val)) {
                    message = "# Checking databases %s and %s on server1\n#";
                } else {
                    message = ("# Checking databases %s on server1 and %s on server2\n#");
                }

                System.out.printf((message) + "%n", db1Conn.getCatalog(), db2Conn.getCatalog());
            }

            // 有问题的话到不了这一步，直接忽略了
            //checkDatabases();

            //_check_objects
            CheckResult checkResult = checkObjects(db1Conn, db2Conn, db1, db2, options);

            // 这里是python中第一次出现success
            success = !checkResult.isDiffers();

            // reporter 用于打印输出，暂时忽略sqlMode
            Reporter reporter = new Reporter(options);
            reporter.printHeading();

            // inBoth格式：   table_type:table_name
            List<String> inBoth = checkResult.getInBoth();

            for (String item : inBoth) {
                List<String> errorList = new ArrayList<>();
                List<String> debugMsgs = new ArrayList<>();

                String objType = item.split(":")[0];

                String qObj1 = String.format("%s.%s", db1, item.split(":")[1]);
                String qObj2 = String.format("%s.%s", db2, item.split(":")[1]);

                List<String> errors = compareObject(db1Conn, db2Conn, qObj1, qObj2, reporter, options, objType);

                errorList.addAll(errors);

                if (objType.equals("TABLE")) {
                    errors = checkRowCounts(db1Conn, db2Conn, qObj1, qObj2, reporter, options);
                    if (!errors.isEmpty()) {
                        errorList.addAll(errors);
                    }
                } else {
                    reporter.reportState("-");
                }

                if (objType.equals("TABLE")) {
                    // 这里应该是由两个返回值的，但是貌似第二个返回值一直为空，且后续没有使用
                    errors = checkDataConsistency(db1Conn, db2Conn, qObj1, qObj2, reporter, options);
                    if (!errors.isEmpty()) {
                        errorList.addAll(errors);
                    }
                } else reporter.reportState("-");
                // selectedCode部分的逻辑
                if ((int) options.get("verbosity") > 0) {
                    if (!quiet) {
                        System.out.println();
                    }
                    try {
                        String createObj1 = DbCompareUtils.getCreateObject(db1Conn, qObj1, options, objType);
                        String createObj2 = DbCompareUtils.getCreateObject(db2Conn, qObj2, options, objType);

                    } catch (SQLException e) {
                        System.err.println("Error getting object definition: " + e.getMessage());
                    }
                }

                if (debugMsgs != null && !debugMsgs.isEmpty() && (int) options.get("verbosity") > 2) {
                    reporter.reportErrors(debugMsgs);
                }

                if (!quiet) {
                    reporter.reportErrors(errorList);
                }
                if (!errorList.isEmpty()) {
                    success = false;
                }

            }
            return success;
        }
    }

    /**
     * 对比两个服务器上所有非系统数据库
     *
     * @param server1Val 服务器1连接信息
     * @param server2Val 服务器2连接信息
     * @param excludeList 要排除的数据库列表
     * @param options 配置选项
     * @return 对比结果 - true表示全部匹配，false表示有差异，null表示没有可比较的数据库
     */
    public boolean compareAllDatabases(DbConnection server1Val, DbConnection server2Val, List<String> excludeList, Map<String, Object> options) throws SQLException {
        boolean success = true;
        boolean quiet = (boolean) options.getOrDefault("quiet", false);

        // 这里的server1Val只是装饰的，实际应该用的Connection

        try (Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "infocore");
             Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3307", "root", "infocore")) {

            // 检查指定的服务器是否相同，这里简化处理
            if (conn1.equals(conn2)) {
                throw new SQLException(
                        "Specified servers are the same (server1=localhost:3306 and " +
                                "server2=localhost:3307). Cannot compare all databases on the same server."
                );
            }

            // 获取除了排除的所有数据库
            StringBuilder conditions = new StringBuilder();
            if (excludeList != null && !excludeList.isEmpty()) {
                // 添加WHERE条件排除指定数据库
                String operator = (boolean) options.getOrDefault("use_regexp", false) ? "REGEXP" : "LIKE";
                List<String> excludeConditions = new ArrayList<>();
                for (String db : excludeList) {
                    excludeConditions.add(String.format("SCHEMA_NAME NOT %s '%s'", operator, db));
                }
                conditions.append("AND ").append(String.join(" AND ", excludeConditions));
            }

            // 构建查询语句
            String getDbsQuery = """
                        SELECT SCHEMA_NAME
                        FROM INFORMATION_SCHEMA.SCHEMATA
                        WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
                        AND SCHEMA_NAME != 'PERFORMANCE_SCHEMA'
                        AND SCHEMA_NAME != 'mysql'
                        AND SCHEMA_NAME != 'sys'
                        %s
                    """.formatted(conditions.toString());

            // 获取服务器1上的数据库
            Set<String> server1Dbs = new HashSet<>();
            try (PreparedStatement stmt1 = conn1.prepareStatement(getDbsQuery);
                 ResultSet rs1 = stmt1.executeQuery()) {
                while (rs1.next()) {
                    server1Dbs.add(rs1.getString(1));
                }
            }

            // 获取服务器2上的数据库
            Set<String> server2Dbs = new HashSet<>();
            try (PreparedStatement stmt2 = conn2.prepareStatement(getDbsQuery);
                 ResultSet rs2 = stmt2.executeQuery()) {
                while (rs2.next()) {
                    server2Dbs.add(rs2.getString(1));
                }
            }

            // 检查缺失的数据库
            String direction = (String) options.getOrDefault("changes-for", "server1");
            if ("server1".equals(direction)) {
                Set<String> diffDbs = new HashSet<>(server1Dbs);
                diffDbs.removeAll(server2Dbs);
                for (String db : diffDbs) {
                    String msg = String.format("The database %s on server1 does not exist on server2.", db);
                    if (!quiet) {
                        System.out.println("# " + msg);
                    }
                }
            } else {
                Set<String> diffDbs = new HashSet<>(server2Dbs);
                diffDbs.removeAll(server1Dbs);
                for (String db : diffDbs) {
                    String msg = String.format("The database %s on server2 does not exist on server1.", db);
                    if (!quiet) {
                        System.out.println("# " + msg);
                    }
                }
            }

            Set<String> commonDbs = new HashSet<>(server1Dbs);
            commonDbs.retainAll(server2Dbs);

            if (!commonDbs.isEmpty()) {
                if (!quiet) {
                    System.out.println("# Comparing databases: " + String.join(", ", commonDbs));
                }
            } else {
                success = false;
            }

            for (String db : commonDbs) {
                try {
                    boolean res = databaseCompare(server1Val, server2Val, db, db, options);
                    if (!res) {
                        success = false;
                    }
                    if (!quiet) {
                        System.out.println("\n");
                    }
                } catch (SQLException e) {
                    System.out.println("ERROR: " + e.getMessage() + "\n");
                    success = false;
                }
            }

            return success;
        }
    }


    private List<String> checkDataConsistency(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Reporter reporter, Map<String, Object> options) {

        String direction = (String) options.getOrDefault("changes-for", "server1");
        boolean reverse = (boolean) options.getOrDefault("reverse", false);
        boolean quiet = (boolean) options.getOrDefault("quiet", false);

        List<String> errors = new ArrayList<>();
        if (!(boolean) options.get("no_data")) {
            reporter.reportState("-");
            try {
                DbCompareUtils.DiffServer diffServer = DbCompareUtils.checkConsistency(db1Conn, db2Conn, obj1, obj2, options, reporter);
                // if no differences, return
                if ((diffServer == null) ||
                        (!reverse && "server1".equals(direction) && diffServer.getFirst() == null) ||
                        (!reverse && "server2".equals(direction) && diffServer.getSecond() == null)) {
                    // 返回空的错误列表，表示没有差异
                    return new ArrayList<>();
                }

                // 如果存在差异，根据方向构建差异列表
                if (direction.equals("server1") || reverse) {
                    if (diffServer.getFirst() != null) {
                        errors.addAll(diffServer.getFirst());
                    }
                }

                if (direction.equals("server2") || reverse) {
                    if (diffServer.getSecond() != null) {
                        errors.addAll(diffServer.getSecond());
                    }
                }
            } catch (SQLException e) {
                // 处理异常情况
                if (e.getMessage().endsWith("not have an usable Index or primary key.")) {
                    reporter.reportState("SKIP");
                    errors.add("# " + e.getMessage());
                } else {
                    reporter.reportState("FAIL");
                    errors.add(e.getMessage());
                }
            }
        } else {
            reporter.reportState("SKIP");
        }

        return errors;
    }

    private List<String> checkRowCounts(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Reporter reporter, Map<String, Object> options) throws SQLException {

        List<String> errors = new ArrayList<>();
        if (!(boolean) options.get("no_row_count")) {
            try (PreparedStatement statement1 = db1Conn.prepareStatement("SELECT COUNT(*) FROM " + obj1);
                 PreparedStatement statement2 = db2Conn.prepareStatement("SELECT COUNT(*) FROM " + obj2);
                 ResultSet resultSet1 = statement1.executeQuery();
                 ResultSet resultSet2 = statement2.executeQuery();
            ) {
                String rs1 = null, rs2 = null;
                // count结果应该在第二列
                if (resultSet1.next()) rs1 = resultSet1.getString(1);
                if (resultSet2.next()) rs2 = resultSet2.getString(1);
                if (rs1 != null && rs2 != null && !rs1.equals(rs2)) {
                    reporter.reportState("FAIL");
                    String msg = String.format(ERROR_ORW_COUNT, obj1, obj2);
                    if (!(boolean) options.get("run_all_tests") && !(boolean) options.getOrDefault("quiet", false)) {
                        //throw new Error(msg);
                        System.err.println(msg);
                    } else {
                        errors.add(String.format("# %s", msg));
                    }
                } else {
                    reporter.reportState("PASS");
                }
            }
        } else {
            reporter.reportState("SKIP");
        }
        return errors;
    }

    private List<String> compareObject(Connection db1Conn, Connection db2Conn, String obj1, String obj2, Reporter reporter, Map<String, Object> options, String objType) throws SQLException {

        List<String> errors = new ArrayList<>();
        if (!(boolean) options.get("no_diff")) {
            Map<String, Object> newOpt = new HashMap<>(options);
            newOpt.replace("quiet", true);
            newOpt.replace("suppress_sql", true);

            List<String> res = DbCompareUtils.diffObjects(db1Conn, db2Conn, obj1, obj2, newOpt, objType);

            if (!Objects.isNull(res)) {
                reporter.reportState("FAIL");
                errors.addAll(res);
                if (!(boolean) options.get("run_all_tests") && !(boolean) options.getOrDefault("quiet", false)) {
                    throw new Error("The object definitions do not match.");
                }
            } else {
                reporter.reportState("PASS");
            }
        } else {
            reporter.reportState("SKIP");
        }

        return errors;
    }


    @Getter
    @Setter
    class Reporter {

        private int width;
        private String reportState;
        private String reportObject;
        private boolean quiet = false;
        private int opeWidth = 10;
        private int typeWidth = 15;  // 对象类型列宽度
        private int descWidth = 50;  // 描述列宽度

        Reporter(Map<String, Object> options) {
            this.width = (int) options.getOrDefault("width", PRINT_WIDTH) -2;
            this.quiet = (boolean) options.getOrDefault("quiet", false);
        }

        /**
         * 左对齐打印输出
         */
        public void reportState(String state) {
            if (quiet) {
                return;
            }
            System.out.printf("%-" + opeWidth + "s", state);
        }

        public void reportObject(String objType, String description) {
            if (quiet) {
                return;
            }
            System.out.printf("\n# %-" + typeWidth + "s %-" + descWidth + "s", objType, description);
        }

        public void reportErrors(List<String> errors) {
            if (errors != null && !errors.isEmpty()) {
                System.out.println("\n#");
                for (String line : errors) {
                    System.out.println(line);
                }
            }
        }

        public void printHeading() {
            if (quiet) {
                return;
            }
            System.out.printf(ROW_FORMAT1 + "%n",
                    String.format("%-" + typeWidth + "s", ""),
                    String.format("%-" + descWidth + "s", ""),
                    String.format("%-" + opeWidth + "s", "Defn"),
                    String.format("%-" + opeWidth + "s", "Row"),
                    String.format("%-" + opeWidth + "s", "Data"));
            System.out.printf(ROW_FORMAT1 + "%n",
                    String.format("%-" + typeWidth + "s", "Type"),
                    String.format("%-" + descWidth + "s", "Object Name"),
                    String.format("%-" + opeWidth + "s", "Diff"),
                    String.format("%-" + opeWidth + "s", "Count"),
                    String.format("%-" + opeWidth + "s", "Check"));


            System.out.printf("# %s\n",width);
        }
    }

    private CheckResult checkObjects(Connection db1Conn, Connection db2Conn, String db1, String db2, Map<String, Object> options) throws SQLException {

        //get_common_objects
        List<List<String>> temp = DbCompareUtils.getCommonObjects(db1Conn, db2Conn, false, options);
        List<String> inBoth = temp.get(0);
        List<String> inDb1 = temp.get(1);
        List<String> inDb2 = temp.get(2);
        Collections.sort(inBoth);
        boolean differs = false;
        if (!(boolean) options.get("no_object_check")) {
            String server1Str = "server1." + db1;
            String server2Str;

            if (db1Conn.equals(db2Conn)) {
                server2Str = "server1." + db2;
            } else {
                server2Str = "server2." + db2;
            }

            // 检查是否有缺失的对象
            if (!inDb1.isEmpty() || !inDb2.isEmpty()) {
                if ((boolean) options.get("run_all_tests")) {
                    if (!inDb1.isEmpty()) {
                        differs = true;
                        if (!(boolean) options.get("quiet")) {
                            DbCompareUtils.printMissingList(inDb1, server1Str, server2Str);
                            System.out.println("#");
                        }
                    }

                    if (!inDb2.isEmpty()) {
                        differs = true;
                        if (!(boolean) options.get("quiet")) {
                            DbCompareUtils.printMissingList(inDb2, server2Str, server1Str);
                            System.out.println("#");
                        }
                    }
                } else {
                    differs = true;
                    if (!(boolean) options.get("quiet")) {
                        System.err.println(String.format(ERROR_OBJECT_LIST, db1, db2));
                    }
                }
            }

        }

        if ((int) options.get("verbosity") > 1) {
            Map<String, Integer> objects = new HashMap<>();
            objects.put("TABLE", 0);
            objects.put("VIEW", 0);
            objects.put("TRIGGER", 0);
            objects.put("PROCEDURE", 0);
            objects.put("FUNCTION", 0);
            objects.put("EVENT", 0);

            // 统计共同对象的类型
            for (String item : inBoth) {
                String[] parts = item.split(":");
                String objType = parts.length >= 1 ? parts[0] : "unknown";
                if (objects.containsKey(objType)) {
                    objects.put(objType, objects.get(objType) + 1);
                }
            }

            System.out.println("Looking for object types table, view, trigger, procedure, function.");
            System.out.println("Object types found common to both databases:");

            for (Map.Entry<String, Integer> entry : objects.entrySet()) {
                System.out.printf(" %12s : %d%n", entry.getKey(), entry.getValue());
            }
        }

        return new CheckResult(inBoth, inDb1, inDb2, differs);

    }

    private void checkOptionDefault(Map<String, Object> options) {
        options.putAll(DEFAULT_OPTIONS);
    }


}
