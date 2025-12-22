package cn.info.verify;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Set;

@Setter
@Getter
public class SpanData {
    private List<String[]> rowData;
    private Set<RowSignature> signatures;

    // 构造函数、getter/setter
    public SpanData(List<String[]> rowData, Set<RowSignature> signatures) {
        this.rowData = rowData;
        this.signatures = signatures;
    }
}
