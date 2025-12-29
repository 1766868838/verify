package cn.infocore.dbs.compare.model.dto;

import cn.infocore.dbs.compare.model.ObjectDiff;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class DbResultDto {

    private Long id;

    private String sourceDb;
    private String targetDb;

    private int sourceTable;
    private int sourceCount;
    private int sourceView;
    private int sourceFunction;
    private int sourceProcedure;
    private int sourceTrigger;

    private int targetTable;
    private int targetCount;
    private int targetView;
    private int targetFunction;
    private int targetProcedure;
    private int targetTrigger;

    /**
     * 用存储具体的差异，通过length获取差异个数
     */
    private List<ObjectDiff> dbObjectDiff;

    /**
     * 修复语句
     */
    private String repairSql;

}
