package cn.info.verify;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Service;

import java.util.Map;

@Setter
@Getter
public class CompareTable {
    private String compareSign;  // 所有字段的MD5
    private String pk_hash;  // 主键的MD5
    private Map<String, Object> pkValues;  // 主键字段值
    private String span;         // span值

    public CompareTable(String compareSign,
                      Map<String, Object> pkValues, String span) {
        this.compareSign = compareSign;
        this.pkValues = pkValues;
        this.span = span;
    }
    public CompareTable(String compareSign,String pk_hash,
                        Map<String, Object> pkValues, String span) {
        this.compareSign = compareSign;
        this.pk_hash = pk_hash;
        this.pkValues = pkValues;
        this.span = span;
    }
    public CompareTable(){}
}