package cn.infocore.dbs.compare.dao;

import cn.infocore.dbs.compare.model.DbResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface DbResultDao extends JpaRepository<DbResult, Long>, PagingAndSortingRepository<DbResult, Long>, JpaSpecificationExecutor<DbResult> {
}
