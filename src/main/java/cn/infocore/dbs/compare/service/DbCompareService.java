package cn.infocore.dbs.compare.service;

import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;

import java.sql.SQLException;
import java.util.List;

public interface DbCompareService {

    void create(DbCompareDto dbCompare);

    void delete(Long id);

    void update(DbCompareDto dbCompare);

    List<DbCompare> list();

    void start(DbCompareDto dbCompare) throws SQLException;

    void restart(DbCompareDto dbCompare) throws SQLException;

    void stop();
}
