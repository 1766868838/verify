package cn.infocore.dbs.compare.verify.compare;

import jakarta.persistence.criteria.CriteriaBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class CheckResult {
    private List<String> inBoth;
    private List<String> inDb1;
    private List<String> inDb2;
    private Map<String, Integer> sourceObject;
    private Map<String, Integer> targetObject;

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

    public CheckResult(List<String> inBoth, List<String> inDb1, List<String> inDb2, Map<String, Integer> sourceObject, Map<String, Integer> targetObject, boolean differs) {
        this.inBoth = inBoth;
        this.inDb1 = inDb1;
        this.inDb2 = inDb2;
        this.sourceObject = sourceObject;
        this.targetObject = targetObject;
        this.differs = differs;
    }
}
