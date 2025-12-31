package cn.infocore.dbs.compare.service;

import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.dto.DbResultDto;

public interface DbResultService {

    DbResult get(Long id);

    void insert(DbResult dbResult);
}
