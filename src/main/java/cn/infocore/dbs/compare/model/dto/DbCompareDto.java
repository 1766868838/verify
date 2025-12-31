package cn.infocore.dbs.compare.model.dto;

import cn.infocore.dbs.compare.converter.DbObjectConverter;
import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.DbConnection;
import cn.infocore.dbs.compare.model.DbObject;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
public class DbCompareDto implements Serializable{

    private static final long serialVersionUID = 1L;

    public enum TaskStatus {
        PENDING,    // 等待开始
        RUNNING,    // 运行中
        PAUSED,     // 已暂停
        COMPLETED,  // 已完成
        FAILED,     // 已失败
        CANCELLED   // 已取消
    }

    public enum DbType{
        MYSQL, ORACLE
    }

    public enum Type{
        FAST, STRICT
    }

    /**
     * 名称应该是唯一的，通过名称进行状态管理
     */
    private String name;

    @Enumerated(EnumType.STRING)
    private DbType sourceDbType;
    private String sourceHost;
    private int sourcePort;
    private String sourceUsername;
    private String sourcePassword;

    @Enumerated(EnumType.STRING)
    private DbType targetDbType;
    private String targetHost;
    private int targetPort;
    private String targetUsername;
    private String targetPassword;

    @Enumerated(EnumType.STRING)
    private Type type;

    /**
     * 备注
     */
    private String remark;

    /**
     * 校验对象,结构可能如下
     */
    @Convert(converter = DbObjectConverter.class)
    private DbObject object;

    private int concurrentNum;

    //@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "UTC")
    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date startTime;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date endTime;

    private TaskStatus taskStatus;
}
