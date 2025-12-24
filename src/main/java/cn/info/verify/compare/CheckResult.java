package cn.info.verify.compare;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class CheckResult {
    private List<String> inBoth;
    private boolean differs;

    public CheckResult(List<String> inBoth, boolean differs) {
        this.inBoth = inBoth;
        this.differs = differs;
    }
}
