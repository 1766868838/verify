package cn.info.verify;

import org.springframework.boot.SpringApplication;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.sql.*;
import java.util.*;

/**
 * 数据库对象获取器 - 类似MySQL Utilities的get_db_objects功能
 */
public class MySQLDatabaseInspector {

    public enum ObjectType {
        TABLE, VIEW, TRIGGER, PROCEDURE, FUNCTION, EVENT, GRANT
    }

    public enum ColumnMode {
        NAMES,      // 只返回名称
        BRIEF,      // 简要信息
        FULL        // 完整信息
    }

    private final Connection connection;
    private final String schema;
    private final Set<String> excludePatterns = new HashSet<>();

    /**
     * 构造器
     */
    public MySQLDatabaseInspector(Connection connection, String schema) {
        this.connection = connection;
        this.schema = schema;
    }

    /**
     * 添加排除模式（支持通配符）
     */
    public void addExcludePattern(String pattern) {
        excludePatterns.add(pattern);
    }

    /**
     * 获取数据库对象（主方法）
     */
    public ObjectResult getDatabaseObjects(ObjectType type,
                                           ColumnMode columnMode,
                                           boolean needBacktick)
            throws SQLException {

        return getDatabaseObjects(type, columnMode, false, needBacktick);
    }

    /**
     * 获取数据库对象（完整参数）
     */
    public ObjectResult getDatabaseObjects(ObjectType type,
                                           ColumnMode columnMode,
                                           boolean getColumnNames,
                                           boolean needBacktick)
            throws SQLException {

        return switch (type) {
            case TABLE -> fetchTables(columnMode, getColumnNames, needBacktick);
            case VIEW -> fetchViews(columnMode, getColumnNames, needBacktick);
            case TRIGGER -> fetchTriggers(columnMode, getColumnNames, needBacktick);
            case PROCEDURE -> fetchProcedures(columnMode, getColumnNames, needBacktick);
            case FUNCTION -> fetchFunctions(columnMode, getColumnNames, needBacktick);
            case EVENT -> fetchEvents(columnMode, getColumnNames, needBacktick);
            case GRANT -> fetchGrants(columnMode, getColumnNames, needBacktick);
        };
    }

    // ============= TABLE 相关方法 =============

    private ObjectResult fetchTables(ColumnMode mode, boolean getColumnNames,
                                     boolean needBacktick) throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();
        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = null;

        try (ResultSet rs = metaData.getTables(schema, null, "%",
                new String[]{"TABLE"})) {

            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();

            // 获取列名（如果需要）
            if (getColumnNames) {
                columnNames = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnNames.add(rsMeta.getColumnName(i));
                }
            }

            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");

                // 检查排除模式
                if (shouldExclude("TABLE", tableName)) {
                    continue;
                }

                Map<String, Object> tableInfo = new LinkedHashMap<>();

                switch (mode) {
                    case NAMES -> tableInfo.put("TABLE_NAME",
                            needBacktick ? quoteIdentifier(tableName) : tableName);
                    case BRIEF -> {
                        tableInfo.put("TABLE_SCHEMA", schema);
                        tableInfo.put("TABLE_NAME",
                                needBacktick ? quoteIdentifier(tableName) : tableName);

                        // 获取表的基本信息
                        addTableBriefInfo(tableInfo, metaData, tableName);
                    }
                    case FULL -> {
                        // 获取所有列信息
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsMeta.getColumnName(i);
                            Object value = rs.getObject(i);
                            tableInfo.put(colName, value);
                        }

                        // 添加额外信息
                        addTableFullInfo(tableInfo, metaData, tableName, needBacktick);
                    }
                }

                results.add(tableInfo);
            }
        }

        return new ObjectResult(columnNames, results);
    }

    private void addTableBriefInfo(Map<String, Object> tableInfo,
                                   DatabaseMetaData metaData,
                                   String tableName) throws SQLException {

        // 获取表类型
        try (ResultSet rs = connection.createStatement().executeQuery(
                String.format("SHOW TABLE STATUS LIKE '%s'", tableName))) {
            if (rs.next()) {
                tableInfo.put("ENGINE", rs.getString("Engine"));
                tableInfo.put("TABLE_ROWS", rs.getLong("Rows"));
                tableInfo.put("AVG_ROW_LENGTH", rs.getLong("Avg_row_length"));
                tableInfo.put("DATA_LENGTH", rs.getLong("Data_length"));
                tableInfo.put("INDEX_LENGTH", rs.getLong("Index_length"));
                tableInfo.put("TABLE_COLLATION", rs.getString("Collation"));
            }
        }

        // 获取列信息
        List<Map<String, Object>> columns = getTableColumns(metaData, tableName, false);
        tableInfo.put("COLUMNS", columns);
    }

    private void addTableFullInfo(Map<String, Object> tableInfo,
                                  DatabaseMetaData metaData,
                                  String tableName,
                                  boolean needBacktick) throws SQLException {

        // 获取列信息（详细）
        List<Map<String, Object>> columns = getTableColumns(metaData, tableName, true);
        tableInfo.put("COLUMNS_DETAIL", columns);

        // 获取主键
        List<String> primaryKeys = getPrimaryKeys(metaData, tableName);
        tableInfo.put("PRIMARY_KEYS",
                needBacktick ? quoteIdentifiers(primaryKeys) : primaryKeys);

        // 获取外键
        List<Map<String, Object>> foreignKeys = getForeignKeys(metaData, tableName);
        tableInfo.put("FOREIGN_KEYS", foreignKeys);

        // 获取索引
        List<Map<String, Object>> indexes = getIndexes(metaData, tableName);
        tableInfo.put("INDEXES", indexes);

        // 获取创建选项
        try (ResultSet rs = connection.createStatement().executeQuery(
                String.format("SHOW CREATE TABLE `%s`", tableName))) {
            if (rs.next()) {
                tableInfo.put("CREATE_STATEMENT", rs.getString(2));
            }
        }
    }

    private List<Map<String, Object>> getTableColumns(DatabaseMetaData metaData,
                                                      String tableName,
                                                      boolean fullDetails)
            throws SQLException {

        List<Map<String, Object>> columns = new ArrayList<>();

        try (ResultSet rs = metaData.getColumns(schema, null, tableName, "%")) {
            while (rs.next()) {
                Map<String, Object> column = new LinkedHashMap<>();

                if (fullDetails) {
                    column.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    column.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
                    column.put("TYPE_NAME", rs.getString("TYPE_NAME"));
                    column.put("COLUMN_SIZE", rs.getInt("COLUMN_SIZE"));
                    column.put("DECIMAL_DIGITS", rs.getInt("DECIMAL_DIGITS"));
                    column.put("NULLABLE", rs.getInt("NULLABLE"));
                    column.put("REMARKS", rs.getString("REMARKS"));
                    column.put("COLUMN_DEF", rs.getString("COLUMN_DEF"));
                    column.put("ORDINAL_POSITION", rs.getInt("ORDINAL_POSITION"));
                    column.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
                    column.put("IS_AUTOINCREMENT", rs.getString("IS_AUTOINCREMENT"));
                } else {
                    column.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    column.put("TYPE_NAME", rs.getString("TYPE_NAME"));
                    column.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
                    column.put("COLUMN_DEF", rs.getString("COLUMN_DEF"));
                }

                columns.add(column);
            }
        }

        return columns;
    }

    private List<String> getPrimaryKeys(DatabaseMetaData metaData, String tableName)
            throws SQLException {

        List<String> primaryKeys = new ArrayList<>();

        try (ResultSet rs = metaData.getPrimaryKeys(schema, null, tableName)) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        }

        return primaryKeys;
    }

    private List<Map<String, Object>> getForeignKeys(DatabaseMetaData metaData,
                                                     String tableName)
            throws SQLException {

        List<Map<String, Object>> foreignKeys = new ArrayList<>();

        try (ResultSet rs = metaData.getImportedKeys(schema, null, tableName)) {
            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> fk = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    fk.put(rsMeta.getColumnName(i), rs.getObject(i));
                }
                foreignKeys.add(fk);
            }
        }

        return foreignKeys;
    }

    private List<Map<String, Object>> getIndexes(DatabaseMetaData metaData,
                                                 String tableName)
            throws SQLException {

        List<Map<String, Object>> indexes = new ArrayList<>();

        try (ResultSet rs = metaData.getIndexInfo(schema, null, tableName,
                false, true)) {
            while (rs.next()) {
                Map<String, Object> index = new LinkedHashMap<>();
                index.put("INDEX_NAME", rs.getString("INDEX_NAME"));
                index.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                index.put("NON_UNIQUE", rs.getBoolean("NON_UNIQUE"));
                index.put("TYPE", rs.getShort("TYPE"));
                indexes.add(index);
            }
        }

        return indexes;
    }

    // ============= VIEW 相关方法 =============

    private ObjectResult fetchViews(ColumnMode mode, boolean getColumnNames,
                                    boolean needBacktick) throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();
        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = null;

        try (ResultSet rs = metaData.getTables(schema, null, "%",
                new String[]{"VIEW"})) {

            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();

            if (getColumnNames) {
                columnNames = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnNames.add(rsMeta.getColumnName(i));
                }
            }

            while (rs.next()) {
                String viewName = rs.getString("TABLE_NAME");

                if (shouldExclude("VIEW", viewName)) {
                    continue;
                }

                Map<String, Object> viewInfo = new LinkedHashMap<>();

                switch (mode) {
                    case NAMES -> viewInfo.put("TABLE_NAME",
                            needBacktick ? quoteIdentifier(viewName) : viewName);
                    case BRIEF -> {
                        viewInfo.put("TABLE_SCHEMA", schema);
                        viewInfo.put("TABLE_NAME",
                                needBacktick ? quoteIdentifier(viewName) : viewName);

                        // 获取视图定义
                        addViewBriefInfo(viewInfo, viewName);
                    }
                    case FULL -> {
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsMeta.getColumnName(i);
                            viewInfo.put(colName, rs.getObject(i));
                        }

                        // 获取完整的视图信息
                        addViewFullInfo(viewInfo, viewName);
                    }
                }

                results.add(viewInfo);
            }
        }

        return new ObjectResult(columnNames, results);
    }

    private void addViewBriefInfo(Map<String, Object> viewInfo, String viewName)
            throws SQLException {

        String sql = "SELECT VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, " +
                "DEFINER, SECURITY_TYPE " +
                "FROM INFORMATION_SCHEMA.VIEWS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, viewName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    viewInfo.put("VIEW_DEFINITION", rs.getString("VIEW_DEFINITION"));
                    viewInfo.put("CHECK_OPTION", rs.getString("CHECK_OPTION"));
                    viewInfo.put("IS_UPDATABLE", rs.getString("IS_UPDATABLE"));
                    viewInfo.put("DEFINER", rs.getString("DEFINER"));
                    viewInfo.put("SECURITY_TYPE", rs.getString("SECURITY_TYPE"));
                }
            }
        }
    }

    private void addViewFullInfo(Map<String, Object> viewInfo, String viewName)
            throws SQLException {

        String sql = "SELECT * FROM INFORMATION_SCHEMA.VIEWS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, viewName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    int columnCount = rsMeta.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String colName = rsMeta.getColumnName(i);
                        viewInfo.put(colName, rs.getObject(i));
                    }
                }
            }
        }
    }

    // ============= TRIGGER 相关方法 =============

    private ObjectResult fetchTriggers(ColumnMode mode, boolean getColumnNames,
                                       boolean needBacktick) throws SQLException {

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = null;

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS " +
                "WHERE TRIGGER_SCHEMA = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                int columnCount = rsMeta.getColumnCount();

                if (getColumnNames) {
                    columnNames = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnNames.add(rsMeta.getColumnName(i));
                    }
                }

                while (rs.next()) {
                    String triggerName = rs.getString("TRIGGER_NAME");

                    if (shouldExclude("TRIGGER", triggerName)) {
                        continue;
                    }

                    Map<String, Object> triggerInfo = new LinkedHashMap<>();

                    switch (mode) {
                        case NAMES:
                            triggerInfo.put("TRIGGER_NAME",
                                    needBacktick ? quoteIdentifier(triggerName) : triggerName);
                            break;

                        case BRIEF:
                            triggerInfo.put("TRIGGER_SCHEMA", schema);
                            triggerInfo.put("TRIGGER_NAME",
                                    needBacktick ? quoteIdentifier(triggerName) : triggerName);
                            triggerInfo.put("EVENT_MANIPULATION",
                                    rs.getString("EVENT_MANIPULATION"));
                            triggerInfo.put("EVENT_OBJECT_TABLE",
                                    rs.getString("EVENT_OBJECT_TABLE"));
                            triggerInfo.put("ACTION_TIMING",
                                    rs.getString("ACTION_TIMING"));
                            triggerInfo.put("ACTION_STATEMENT",
                                    rs.getString("ACTION_STATEMENT"));
                            break;

                        case FULL:
                            for (int i = 1; i <= columnCount; i++) {
                                String colName = rsMeta.getColumnName(i);
                                triggerInfo.put(colName, rs.getObject(i));
                            }
                            break;
                    }

                    results.add(triggerInfo);
                }
            }
        }

        return new ObjectResult(columnNames, results);
    }

    // ============= PROCEDURE 和 FUNCTION 相关方法 =============

    private ObjectResult fetchProcedures(ColumnMode mode, boolean getColumnNames,
                                         boolean needBacktick) throws SQLException {
        return fetchRoutines("PROCEDURE", mode, getColumnNames, needBacktick);
    }

    private ObjectResult fetchFunctions(ColumnMode mode, boolean getColumnNames,
                                        boolean needBacktick) throws SQLException {
        return fetchRoutines("FUNCTION", mode, getColumnNames, needBacktick);
    }

    private ObjectResult fetchRoutines(String routineType, ColumnMode mode,
                                       boolean getColumnNames, boolean needBacktick)
            throws SQLException {

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = null;

        String sql = "SELECT * FROM INFORMATION_SCHEMA.ROUTINES " +
                "WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, routineType);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                int columnCount = rsMeta.getColumnCount();

                if (getColumnNames) {
                    columnNames = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnNames.add(rsMeta.getColumnName(i));
                    }
                }

                while (rs.next()) {
                    String routineName = rs.getString("ROUTINE_NAME");

                    if (shouldExclude(routineType, routineName)) {
                        continue;
                    }

                    Map<String, Object> routineInfo = new LinkedHashMap<>();

                    switch (mode) {
                        case NAMES -> routineInfo.put("ROUTINE_NAME",
                                needBacktick ? quoteIdentifier(routineName) : routineName);
                        case BRIEF -> {
                            routineInfo.put("ROUTINE_SCHEMA", schema);
                            routineInfo.put("ROUTINE_NAME",
                                    needBacktick ? quoteIdentifier(routineName) : routineName);
                            routineInfo.put("ROUTINE_TYPE", routineType);
                            routineInfo.put("DATA_TYPE", rs.getString("DATA_TYPE"));
                            routineInfo.put("ROUTINE_DEFINITION",
                                    rs.getString("ROUTINE_DEFINITION"));
                            routineInfo.put("SECURITY_TYPE",
                                    rs.getString("SECURITY_TYPE"));
                            routineInfo.put("SQL_DATA_ACCESS",
                                    rs.getString("SQL_DATA_ACCESS"));
                        }
                        case FULL -> {
                            for (int i = 1; i <= columnCount; i++) {
                                String colName = rsMeta.getColumnName(i);
                                routineInfo.put(colName, rs.getObject(i));
                            }
                        }
                    }

                    results.add(routineInfo);
                }
            }
        }

        return new ObjectResult(columnNames, results);
    }

    // ============= EVENT 相关方法 =============

    private ObjectResult fetchEvents(ColumnMode mode, boolean getColumnNames,
                                     boolean needBacktick) throws SQLException {

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = null;

        String sql = "SELECT * FROM INFORMATION_SCHEMA.EVENTS " +
                "WHERE EVENT_SCHEMA = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                int columnCount = rsMeta.getColumnCount();

                if (getColumnNames) {
                    columnNames = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnNames.add(rsMeta.getColumnName(i));
                    }
                }

                while (rs.next()) {
                    String eventName = rs.getString("EVENT_NAME");

                    if (shouldExclude("EVENT", eventName)) {
                        continue;
                    }

                    Map<String, Object> eventInfo = new LinkedHashMap<>();

                    switch (mode) {
                        case NAMES:
                            eventInfo.put("EVENT_NAME",
                                    needBacktick ? quoteIdentifier(eventName) : eventName);
                            break;

                        case BRIEF:
                            eventInfo.put("EVENT_SCHEMA", schema);
                            eventInfo.put("EVENT_NAME",
                                    needBacktick ? quoteIdentifier(eventName) : eventName);
                            eventInfo.put("DEFINER", rs.getString("DEFINER"));
                            eventInfo.put("STATUS", rs.getString("STATUS"));
                            eventInfo.put("EVENT_TYPE", rs.getString("EVENT_TYPE"));
                            eventInfo.put("EXECUTE_AT", rs.getTimestamp("EXECUTE_AT"));
                            eventInfo.put("INTERVAL_VALUE", rs.getString("INTERVAL_VALUE"));
                            eventInfo.put("INTERVAL_FIELD", rs.getString("INTERVAL_FIELD"));
                            break;

                        case FULL:
                            for (int i = 1; i <= columnCount; i++) {
                                String colName = rsMeta.getColumnName(i);
                                eventInfo.put(colName, rs.getObject(i));
                            }
                            break;
                    }

                    results.add(eventInfo);
                }
            }
        }

        return new ObjectResult(columnNames, results);
    }

    // ============= GRANT 相关方法 =============

    private ObjectResult fetchGrants(ColumnMode mode, boolean getColumnNames,
                                     boolean needBacktick) throws SQLException {

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> columnNames = Arrays.asList(
                "GRANTEE", "PRIVILEGE_TYPE", "TABLE_SCHEMA",
                "TABLE_NAME", "COLUMN_NAME", "ROUTINE_NAME"
        );

        String sql = """
            (
                SELECT GRANTEE, PRIVILEGE_TYPE, TABLE_SCHEMA,
                       NULL as TABLE_NAME, NULL AS COLUMN_NAME,
                       NULL AS ROUTINE_NAME
                FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES
                WHERE TABLE_SCHEMA = ?
            ) UNION ALL (
                SELECT GRANTEE, PRIVILEGE_TYPE, TABLE_SCHEMA, TABLE_NAME,
                       NULL, NULL
                FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
                WHERE TABLE_SCHEMA = ?
            ) UNION ALL (
                SELECT GRANTEE, PRIVILEGE_TYPE, TABLE_SCHEMA, TABLE_NAME,
                       COLUMN_NAME, NULL
                FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
                WHERE TABLE_SCHEMA = ?
            ) ORDER BY GRANTEE, PRIVILEGE_TYPE, TABLE_SCHEMA,
                       TABLE_NAME, COLUMN_NAME, ROUTINE_NAME
            """;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, schema);
            pstmt.setString(3, schema);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> grantInfo = new LinkedHashMap<>();

                    for (String columnName : columnNames) {
                        Object value = rs.getObject(columnName);
                        grantInfo.put(columnName, value);
                    }

                    results.add(grantInfo);
                }
            }
        }

        if (!getColumnNames) {
            columnNames = null;
        }

        return new ObjectResult(columnNames, results);
    }

    // ============= 辅助方法 =============

    private boolean shouldExclude(String objectType, String objectName) {
        if (excludePatterns.isEmpty()) {
            return false;
        }

        for (String pattern : excludePatterns) {
            // 简单的通配符匹配（支持*）
            String regex = pattern.replace(".", "\\.")
                    .replace("*", ".*")
                    .replace("?", ".");

            if (objectName.matches(regex)) {
                return true;
            }
        }

        return false;
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }

        // 处理逗号分隔的多个标识符
        if (identifier.contains(",")) {
            String[] parts = identifier.split(",");
            List<String> quotedParts = new ArrayList<>();
            for (String part : parts) {
                quotedParts.add("`" + part.trim() + "`");
            }
            return String.join(", ", quotedParts);
        }

        return "`" + identifier + "`";
    }

    private List<String> quoteIdentifiers(List<String> identifiers) {
        List<String> quoted = new ArrayList<>();
        for (String identifier : identifiers) {
            quoted.add(quoteIdentifier(identifier));
        }
        return quoted;
    }

    // ============= 结果类 =============

    /**
     * 返回结果包装类
     */
    public record ObjectResult(List<String> columnNames, List<Map<String, Object>> data) {
        public boolean hasColumnNames() {
            return columnNames != null && !columnNames.isEmpty();
        }

        @Override
        public String toString() {
            return String.format("ObjectResult{columns=%d, rows=%d}",
                    columnNames != null ? columnNames.size() : 0,
                    data.size());
        }
    }
}