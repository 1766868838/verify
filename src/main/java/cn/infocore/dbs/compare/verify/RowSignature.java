package cn.infocore.dbs.compare.verify;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class RowSignature {

    private String compareSign;
    private String pkHash;

    // 构造函数、getter/setter
    public RowSignature(String compareSign, String pkHash) {
        this.compareSign = compareSign;
        this.pkHash = pkHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowSignature that)) return false;
        return Objects.equals(compareSign, that.compareSign) && Objects.equals(pkHash, that.pkHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compareSign, pkHash);
    }
}
