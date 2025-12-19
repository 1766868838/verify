package cn.info.verify;

import lombok.Builder;

import java.util.Objects;

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
        return count == that.count && Objects.equals(span, that.span) && Objects.equals(sumPart1, that.sumPart1) && Objects.equals(sumPart2, that.sumPart2) && Objects.equals(sumPart3, that.sumPart3) && Objects.equals(sumPart4, that.sumPart4);
    }

    @Override
    public int hashCode() {
        return Objects.hash(span, count, sumPart1, sumPart2, sumPart3, sumPart4);
    }

    /**
     * 获取完整的签名字符串（四部分用逗号连接）
     */
    public String getFullSignature() {
        return sumPart1 + "," + sumPart2 + "," + sumPart3 + "," + sumPart4;
    }

    /**
     * 获取签名数组
     */
    public Long[] getSignatureArray() {
        return new Long[]{sumPart1, sumPart2, sumPart3, sumPart4};
    }

    public String getSpan() {
        return span;
    }

    public void setSpan(String span) {
        this.span = span;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getSumPart1() {
        return sumPart1;
    }

    public void setSumPart1(Long sumPart1) {
        this.sumPart1 = sumPart1;
    }

    public Long getSumPart2() {
        return sumPart2;
    }

    public void setSumPart2(Long sumPart2) {
        this.sumPart2 = sumPart2;
    }

    public Long getSumPart3() {
        return sumPart3;
    }

    public void setSumPart3(Long sumPart3) {
        this.sumPart3 = sumPart3;
    }

    public Long getSumPart4() {
        return sumPart4;
    }

    public void setSumPart4(Long sumPart4) {
        this.sumPart4 = sumPart4;
    }
}
