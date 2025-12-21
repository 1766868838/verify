package cn.info.verify;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@Builder
public class HashSummaryTable {

    private String span;          // span的十六进制字符串
    private Long count;           // 分组中的行数
    private Long sumPart1;  // 第一部分的和（字符1-8）
    private Long sumPart2;  // 第二部分的和（字符9-16）
    private Long sumPart3;  // 第三部分的和（字符17-24）
    private Long sumPart4;  // 第四部分的和（字符25-32）

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HashSummaryTable that)) return false;
        return Objects.equals(count, that.count) && Objects.equals(span, that.span) && Objects.equals(sumPart1, that.sumPart1) && Objects.equals(sumPart2, that.sumPart2) && Objects.equals(sumPart3, that.sumPart3) && Objects.equals(sumPart4, that.sumPart4);
    }

    @Override
    public int hashCode() {
        return Objects.hash(span, count, sumPart1, sumPart2, sumPart3, sumPart4);
    }
}
