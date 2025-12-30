package cn.infocore.dbs.compare.model;

import cn.infocore.dbs.compare.model.dto.DbResultDto;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Entity
@Getter
@Setter
public class DbResult extends DbResultDto {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Date startTime;
    private Date endTime;


}
