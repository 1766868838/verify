package cn.infocore.dbs.compare.model.vo;

import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DbCompareVo extends DbCompareDto {

    DbResult dbResult;
}
