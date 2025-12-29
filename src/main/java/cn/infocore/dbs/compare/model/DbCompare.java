package cn.infocore.dbs.compare.model;

import cn.infocore.dbs.compare.converter.DbObjectConverter;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

@Entity
@Getter
@Setter
public class DbCompare implements Serializable {

    public enum Type{
        FAST, STRICT
    }
    public enum DbType{
        MYSQL, ORACLE
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

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

    @Convert(converter = DbObjectConverter.class)
    private DbObject object;

    private int concurrentNum;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date startTime;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date endTime;

    /**
     * 对应的最新一条校验结果id
     */
    private Long obResultId;
}
