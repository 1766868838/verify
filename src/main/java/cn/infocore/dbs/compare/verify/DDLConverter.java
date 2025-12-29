package cn.infocore.dbs.compare.verify;

import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.DeltaType;
import com.github.difflib.patch.Patch;
import com.github.difflib.text.DiffRow;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDLConverter {
    public static List<String> getDef(String sql){
        List<String> alterStatements = new ArrayList<>();

        // 解析CREATE TABLE语句
        if (sql.trim().toUpperCase().startsWith("CREATE TABLE")) {
            Pattern tableNamePattern = Pattern.compile(
                    "CREATE\\s+TABLE\\s+(?:IF NOT EXISTS\\s+)?(\\w+)",
                    Pattern.CASE_INSENSITIVE
            );

            Matcher matcher = tableNamePattern.matcher(sql);
            if (matcher.find()) {

                Pattern columnPattern = Pattern.compile(
                        "\\((.*)\\)",
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                );

                Matcher colMatcher = columnPattern.matcher(sql);
                if (colMatcher.find()) {
                    String columnsPart = colMatcher.group(1);
                    String[] definitions = columnsPart.replaceAll("\\n","").split(",");

                    return Arrays.stream(definitions).toList();

                }
            }
        }
        return alterStatements;
    }
    /**
     * 标准化SQL定义字符串以便比较
     */
    private static String normalize(String sql) {
        if (sql == null) return "";
        return sql.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("\\n", " ")
                .toLowerCase();
    }


    private static String extractColumnName(String sql) {
        if (sql == null || sql.trim().isEmpty()) return "";

        String trimmed = sql.trim();
        // 移除开头的换行符
        if (trimmed.startsWith("\n")) {
            trimmed = trimmed.substring(1).trim();
        }

        // 匹配列定义：以单词开头，然后是数据类型
        String[] parts = trimmed.split("\\s+");
        if (parts.length > 0) {
            String firstWord = parts[0].trim();
            // 移除可能的反引号
            firstWord = firstWord.replace("`", "");
            return firstWord;
        }

        return "";
    }

    /**
     * 检查是否是约束定义
     */
    private static boolean isConstraint(String sql) {
        String normalized = normalize(sql);
        return normalized.contains("primary key") ||
                normalized.contains("foreign key") ||
                normalized.contains("constraint") ||
                normalized.contains("key ");
    }

    /**
     * 提取约束名
     */
    private static String extractConstraintName(String sql) {
        // 简单实现，实际需要更复杂的解析
        String[] words = sql.trim().split("\\s+");
        for (int i = 0; i < words.length; i++) {
            if (words[i].equalsIgnoreCase("CONSTRAINT") && i + 1 < words.length) {
                return words[i + 1];
            }
        }
        return null;
    }


    /**
     * 目前只做了表的结构修复语句，并且没有考虑主键的变化
     * @param patch
     * @return
     */
    public static String getRepairSql(Patch<String> patch) {
        StringBuilder sql = new StringBuilder("ALERT TABLE user ");
        for (AbstractDelta<String> delta : patch.getDeltas()) {

            // INSERT代表容灾表多的定义，获取列名和DROP 拼接
            if (delta.getType() == DeltaType.INSERT) {
                // 按行处理不同
                for(String line : delta.getSource().getLines()){
                    String name = Arrays.stream(line.split(" ")).findFirst().toString();
                    sql.append("DROP COLUMN ").append(name).append(",");
                }
            }
            // DELETE代表容灾表少的定义，可以直接和ADD 拼接在一起
            else if (delta.getType() == DeltaType.DELETE) {
                for(String line : delta.getSource().getLines()){
                    sql.append("ADD ").append(line).append(",");
                }
            }
            // CHANGE代表容灾表改变的定义，可能只是多了个空格，这里不关心
            else if (delta.getType() == DeltaType.CHANGE) {
                for(String line : delta.getSource().getLines()){
                    sql.append("CHANGE ").append(line).append(",");
                }
            }
        }
        sql.replace(sql.length()-1,sql.length()-1,";");
        return sql.toString();
    }
}