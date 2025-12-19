package cn.info.verify;

/**
 * 分组数据容器
 */
public class GroupData {
    String spanHex;
    long count;
    Long sum1 = 0L;
    Long sum2 = 0L;
    Long sum3 = 0L;
    Long sum4 = 0L;

    GroupData(String spanHex) {
        this.spanHex = spanHex;
    }

    void incrementCount() {
        count++;
    }

    void addToSums(Long[] parts){
        sum1 += parts[0];
        sum2 += parts[1];
        sum3 += parts[2];
        sum4 += parts[3];
    }
}