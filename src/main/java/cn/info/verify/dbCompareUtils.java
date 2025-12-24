package cn.info.verify;

import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Component
public class dbCompareUtils {


    public static void serverConnect(String server1Val, String server2Val, String db1, String db2, Map<String, Object> options) {
    }


    public static List<List<String>> getCommonObjects(Connection conn1,Connection conn2, boolean printList, Map<String,Object> options) throws SQLException {

        if(Objects.isNull(options)) options = new HashMap<>();

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

        if(printList){
            String server1Str = "server1." + conn1.getCatalog();
            String server2Str;
            if(conn1.equals(conn2)){
                server2Str = "server1." + conn2.getCatalog();
            }
            else {
                server2Str = "server2." + conn2.getCatalog();
            }

            printMissingList(inDb1NotDb2,server1Str,server2Str);
            printMissingList(inDb2NotDb1,server2Str,server1Str);
        }
        List<List<String>> result = new ArrayList<>();
        result.add(inBoth);
        result.add(inDb1NotDb2);
        result.add(inDb2NotDb1);

        return result;
    }

    public static boolean printMissingList(List<String> itemList, String first, String second) {

        if(itemList.size() == 0){
            return false;
        }
        System.out.printf("# WARNING: Objects in %s but not in %s:",first, second);
        for(String item : itemList){
            String[] parts = item.split(":");
            String type = parts[0];
            String name = parts.length > 1 ? parts[1] : "unknown";

            System.out.printf("# %12s: %s", name, type);
        }
        return true;
    }

    /**
     * 原方法返回的是三个列表，直接在主函数中处理
     */
    public static void getCommonList(){}

    /**
     * 拼接table_name 和 table_type
     * @param connection
     * @param databaseName
     * @param options
     * @return table_name:table_type
     * @throws SQLException
     */
    public static List<String> getObjects(Connection connection, String databaseName, Map<String,Object> options) throws SQLException {
        List<String> objects = new ArrayList<>();

        // 这里的options完全没用上
        options.replace("skip_grants", Boolean.TRUE);

        // 获取表
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(databaseName, null, null,
                new String[]{"TABLE", "VIEW", "PROCEDURE", "FUNCTION"});

        while (tables.next()) {
            objects.add(tables.getString("TABLE_TYPE")+":"+tables.getString("TABLE_NAME"));
        }

        // 对结果进行排序
        Collections.sort(objects);

        return objects;
    }
}
