package cn.infocore.dbs.compare.verify.compare;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class CheckResult {
    private List<String> inBoth;
    private List<String> inDb1;
    private List<String> inDb2;
    private boolean differs;

    public CheckResult(List<String> inBoth, boolean differs) {
        this.inBoth = inBoth;
        this.differs = differs;
    }

    public CheckResult(List<String> inBoth, List<String> inDb1, List<String> inDb2, boolean differs) {
        this.inBoth = inBoth;
        this.differs = differs;
        this.inDb1 = inDb1;
        this.inDb2 = inDb2;
    }
}
