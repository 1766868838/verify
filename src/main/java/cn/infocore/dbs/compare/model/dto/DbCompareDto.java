package cn.infocore.dbs.compare.model.dto;

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
public class DbCompareDto implements Serializable {

    private String name;

    private DbConnection sourceDb;

    private DbConnection targetDb;

    @Enumerated(EnumType.STRING)
    private DbCompare.Type type;

    /**
     * 备注
     */
    private String remark;

    /**
     * 校验对象,结构可能如下
     */
    private DbObject object;

    private int concurrentNum;

    //@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "UTC")
    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date startTime;

    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private Date endTime;
}
