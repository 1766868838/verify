package cn.infocore.dbs.compare.verify;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class StructureDiff {

    enum structureType{
        TABLE, FUNCTION, PROCEDURE, VIEW;
    }

    private structureType type;
    private int diffCount = 0;
    private List<String> repairSql = new ArrayList<>();

    public void add(){
        this.diffCount++;
    }
    public void addRepairSql(String repairSql){
        this.repairSql.add(repairSql);
    }

    @Override
    public String toString() {
        return "StructureDiff{" +
                "type=" + type +
                ", diffCount=" + diffCount +
                ", repairSql=" + repairSql +
                '}';
    }
}
