package cn.infocore.dbs.compare.model;

import cn.infocore.dbs.compare.model.dto.DbResultDto;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
public class DbResult extends DbResultDto {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

}
