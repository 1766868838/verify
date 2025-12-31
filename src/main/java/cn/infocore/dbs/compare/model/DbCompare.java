package cn.infocore.dbs.compare.model;

import cn.infocore.dbs.compare.converter.DbObjectConverter;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

@Entity
@Getter
@Setter
public class DbCompare extends DbCompareDto implements Serializable {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    /**
     * 对应的最新一条校验结果id
     */
    private Long obResultId;
}
