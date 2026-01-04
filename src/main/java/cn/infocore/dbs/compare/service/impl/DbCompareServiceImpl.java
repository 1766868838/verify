package cn.infocore.dbs.compare.service.impl;

import cn.infocore.dbs.compare.model.DbConnection;
import cn.infocore.dbs.compare.model.DbResult;
import cn.infocore.dbs.compare.model.ObjectDiff;
import cn.infocore.dbs.compare.model.dto.DbResultDto;
import cn.infocore.dbs.compare.model.vo.DbCompareVo;
import cn.infocore.dbs.compare.verify.DbCompareEntry;
import cn.infocore.dbs.compare.verify.VerifyClient;
import cn.infocore.dbs.compare.converter.DbObjectConverter;
import cn.infocore.dbs.compare.dao.DbCompareDao;
import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import cn.infocore.dbs.compare.service.DbCompareService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
public class DbCompareServiceImpl implements DbCompareService, Job {

    @Autowired
    private DbCompareEntry dbCompareEntry;

    @Autowired
    private DbCompareDao dao;

    @Autowired
    private DbResultServiceImpl dbResultService;

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
    public DbCompareVo get(Long id) {
        DbCompare dbCompare = dao.findById(id).get();
        // result 是通过dbCompare的 dbResult 获取的，存储的是最后一次result的id
        DbResult result = dbResultService.get(dbCompare.getDbResultId());
        DbCompareVo dbCompareVo = new DbCompareVo();
        BeanUtils.copyProperties(dbCompare, dbCompareVo);
        dbCompareVo.setDbResult(result);
        return dbCompareVo;
    }

    @Override
    public void update(DbCompare dbCompare) {
        dao.save(dbCompare);
    }

    @Override
    public List<DbCompare> list() {
        return dao.findAll();
    }

    @Override
    public void restart(DbCompare dbCompare) throws SQLException {
        start(dbCompare);
    }


    @Override
    public void start(DbCompare dbCompare) throws SQLException {



    }


    @Override
    public void execute(JobExecutionContext context) {

        System.out.println("开始执行任务");
        DbCompare dbCompare = (DbCompare) context.getJobDetail().getJobDataMap().get("DbCompare");
        try {
            // 先试试指定数据库比较
            if(dbCompare == null){
                System.err.println("dbCompare 参数不能为 null");
            }
            if (dbCompare.getSourceHost() == null || dbCompare.getSourceHost() == null) {
                System.err.println("源数据库或目标数据库配置不能为 null");
            }
            DbConnection sourceDb = new DbConnection();
            sourceDb.setDbType(dbCompare.getSourceDbType());
            sourceDb.setHost(dbCompare.getSourceHost());
            sourceDb.setPort(dbCompare.getSourcePort());
            sourceDb.setUsername(dbCompare.getSourceUsername());
            sourceDb.setPassword(dbCompare.getSourcePassword());

            DbConnection targetDb = new DbConnection();
            targetDb.setDbType(dbCompare.getTargetDbType());
            targetDb.setHost(dbCompare.getTargetHost());
            targetDb.setPort(dbCompare.getTargetPort());
            targetDb.setUsername(dbCompare.getTargetUsername());
            targetDb.setPassword(dbCompare.getTargetPassword());

            DbResultDto resultDto = dbCompareEntry.databaseCompare(sourceDb, targetDb,
                    "test1", "test2", null,1);
            resultDto.setSourceDb(sourceDb.getHost());
            resultDto.setTargetDb(targetDb.getHost());

            System.out.println(resultDto);

            // 将结果存储到数据库中
            DbResult result = new DbResult();
            BeanUtils.copyProperties(resultDto,result);
            dbResultService.insert(result);

            // 将result id更新到dbCompare 中
            dbCompare.setDbResultId(result.getId());
            update(dbCompare);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
