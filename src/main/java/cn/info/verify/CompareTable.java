package cn.info.verify;

import java.util.Map;

public class CompareTable {
    private String compareSign;  // 所有字段的MD5
    private Map<String, Object> pkValues;  // 主键字段值
    private String span;         // span值

    public CompareTable(String compareSign,
                      Map<String, Object> pkValues, String span) {
        this.compareSign = compareSign;
        this.pkValues = pkValues;
        this.span = span;
    }

    public String getCompareSign() { return compareSign; }
    public Map<String, Object> getPkValues() { return pkValues; }
    public String getSpan() { return span; }

    public void setCompareSign(String compareSign) {
        this.compareSign = compareSign;
    }

    public void setPkValues(Map<String, Object> pkValues) {
        this.pkValues = pkValues;
    }

    public void setSpan(String span) {
        this.span = span;
    }
}