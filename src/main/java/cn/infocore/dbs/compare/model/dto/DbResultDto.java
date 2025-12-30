package cn.infocore.dbs.compare.model.dto;

import cn.infocore.dbs.compare.converter.DbObjectConverter;
import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.ObjectDiff;
import jakarta.persistence.Convert;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DbResultDto{

    private String sourceDb;
    private String targetDb;

    private Map<String,Integer> sourceObject;
//    private int sourceTable;
//    private int sourceCount;
//    private int sourceView;
//    private int sourceFunction;
//    private int sourceProcedure;
//    private int sourceTrigger;

    private Map<String,Integer> targetObject;
//    private int targetTable;
//    private int targetCount;
//    private int targetView;
//    private int targetFunction;
//    private int targetProcedure;
//    private int targetTrigger;

    /**
     * 存储具体的差异，通过length获取差异个数
     */
    @Convert(converter = DbObjectConverter.class)
    private List<ObjectDiff> objectDiff = new ArrayList<>();

    /**
     * 修复语句
     */
    private String repairSql;

    public void addObjectDiff(List<ObjectDiff> objectDiff){
        this.objectDiff.addAll(objectDiff);
    }

    /**
     * 添加sql修复语句
     * @param repairSql
     */
    public void addRepairSql(List<String> repairSql){
        this.repairSql += String.join("",repairSql);
    }

}
