package cn.infocore.dbs.compare.service;

import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;

import java.sql.SQLException;
import java.util.List;

public interface DbCompareService {

    void create(DbCompareDto dbCompare);

    void delete(Long id);

    void update(DbCompare dbCompare);
    void update(DbCompareDto dbCompareDto);

    List<DbCompare> list();

    void start(DbCompare dbCompare) throws SQLException;

    void restart(DbCompare dbCompare) throws SQLException;
}
