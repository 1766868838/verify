package cn.info.verify;

import cn.info.verify.compare.CheckResult;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * 数据库校验入口类
 */
@Component
public class dbCompare {

    public final static int DEFAULT_SPAN_KEY_SIZE = 8;

    private final static String ERROR_DB_MISSING = "The database %s does not exist.";
    private final static String ERROR_OBJECT_LIST = "The list of objects differs among database %s and %s.";
    private final static Map<String, Object> DEFAULT_OPTIONS = new HashMap<>() {{
        put("quiet", false);
        put("verbosity", 0);
        put("difftype", "differ");
        put("run_all_tests", false);
        put("width", 75);
        put("no_object_check", false);
        put("no_diff", false);
        put("no_row_count", false);
        put("no_data", false);
        put("transform", false);
        put("span_key_size", DEFAULT_SPAN_KEY_SIZE);
    }};




    public boolean databaseCompare(String server1Val, String server2Val, String db1, String db2, Map<String,Object> options) throws SQLException {

        checkOptionDefault(options);
        boolean quiet = (boolean) options.getOrDefault("quiet","False");

        // 得到两个数据库
        dbCompareUtils.serverConnect(server1Val,server2Val,db1,db2,options);

        // 直接从conn开始吧
        try(Connection db1Conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test3","root","infocore");
            Connection db2Conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test4","root","infocore")){

            if(db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db1));
            if(db1Conn.isValid(10)) throw new SQLException(String.format(ERROR_DB_MISSING, db2));

            if(!quiet){
                String message;
                if(server2Val.isEmpty()){
                    message = "# Checking databases %s and %s on server1\n#";
                }
                else{
                    message = ("# Checking databases %s on server1 and %s on server2\n#");
                }

                System.out.printf((message) + "%n", db1Conn.getCatalog(), db2Conn.getCatalog());
            }

            // 有问题的话到不了这一步，直接忽略了
            //checkDatabases();

            //_check_objects
            CheckResult checkResult = checkObjects(db1Conn, db2Conn, db1, db2, options);

            boolean success = !checkResult.isDiffers();

            //reporter 貌似没必要, 也忽略sqlMode
            //这里的inBoth格式：   table_type:table_name
            List<String> inBoth = checkResult.getInBoth();

            for(String item : inBoth){
                List<String> errorList = new ArrayList<>();
                List<String> debugMsgs = new ArrayList<>();

                String objType = item.split(":")[0];

            }

        }

        return true;
    }

    private CheckResult checkObjects(Connection db1Conn, Connection db2Conn, String db1, String db2, Map<String,Object>options) throws SQLException {

        //get_common_objects
        List<List<String>> temp = dbCompareUtils.getCommonObjects(db1Conn, db2Conn, false, options);
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
                            dbCompareUtils.printMissingList(inDb1, server1Str, server2Str);
                            System.out.println("#");
                        }
                    }

                    if (!inDb2.isEmpty()) {
                        differs = true;
                        if (!(boolean) options.get("quiet")) {
                            dbCompareUtils.printMissingList(inDb2, server2Str, server1Str);
                            System.out.println("#");
                        }
                    }
                } else {
                    differs = true;
                    if (!(boolean) options.get("quiet")) {
                        throw new SQLException(String.format(ERROR_OBJECT_LIST, db1, db2));
                    }
                }
            }

        }

        if((int)options.get("verbosity")>1){
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
                String objType = parts.length > 1 ? parts[1] : "unknown";
                if (objects.containsKey(objType)) {
                    objects.put(objType, objects.get(objType) + 1);
                }
            }

            System.out.println("Looking for object types table, view, trigger, procedure, function, and event.");
            System.out.println("Object types found common to both databases:");

            for (Map.Entry<String, Integer> entry : objects.entrySet()) {
                System.out.printf(" %12s : %d%n", entry.getKey(), entry.getValue());
            }
        }

        return new CheckResult(inBoth,differs);

    }

    private void checkOptionDefault(Map<String,Object> options) {
        options.putAll(DEFAULT_OPTIONS);
    }


}
