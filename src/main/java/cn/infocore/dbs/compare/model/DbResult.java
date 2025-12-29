package cn.infocore.dbs.compare.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Generated;

@Entity
@Getter
@Setter
public class DbResult {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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
    private String dbObjectDiff;

    /**
     * 修复语句
     */
    private String repairSql;

}
