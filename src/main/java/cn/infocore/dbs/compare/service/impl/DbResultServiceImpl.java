package cn.infocore.dbs.compare.service.impl;

import cn.infocore.dbs.compare.dao.DbResultDao;
import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.dto.DbResultDto;
import cn.infocore.dbs.compare.service.DbResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DbResultServiceImpl implements DbResultService {

    @Autowired
    private DbResultDao dao;

    @Override
    public DbResult get(Long id) {
        return dao.findById(id).get();
    }

    @Override
    public void insert(DbResult dbResult) {
        dao.save(dbResult);
    }
}
