package cn.infocore.dbs.compare.service.impl;

import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.ObjectDiff;
import cn.infocore.dbs.compare.model.dto.DbResultDto;
import cn.infocore.dbs.compare.verify.DbCompareEntry;
import cn.infocore.dbs.compare.verify.VerifyClient;
import cn.infocore.dbs.compare.converter.DbObjectConverter;
import cn.infocore.dbs.compare.dao.DbCompareDao;
import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import cn.infocore.dbs.compare.service.DbCompareService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
public class DbCompareServiceImpl implements DbCompareService {

    @Autowired
    private DbCompareEntry dbCompareEntry;

    @Autowired
    private DbCompareDao dao;

    @Override
    public void create(DbCompareDto dbCompare) {
        update(dbCompare);
    }

    @Override
    public void delete(Long id) {
        dao.deleteById(id);
    }

    @Override
    public void update(DbCompareDto dbCompareDto) {
        DbCompare dbCompare = new DbCompare();
        BeanUtils.copyProperties(dbCompareDto,dbCompare);
        dao.save(dbCompare);
    }

    @Override
    public List<DbCompare> list() {
        return dao.findAll();
    }

    @Override
    public void restart(DbCompareDto dbCompare) throws SQLException {
        start(dbCompare);
    }

    @Override
    public void stop() {

    }

    @Override
    public void start(DbCompareDto dbCompare) throws SQLException {

        // 先试试指定数据库比较
        DbResultDto resultDto = dbCompareEntry.databaseCompare(
                dbCompare.getSourceDb(), dbCompare.getTargetDb(),
                "test3", "test4", null,1);
        resultDto.setSourceDb(dbCompare.getSourceDb().getHost());
        resultDto.setTargetDb(dbCompare.getTargetDb().getHost());

    }


}
