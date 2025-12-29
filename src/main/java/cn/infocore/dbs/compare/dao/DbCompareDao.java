package cn.infocore.dbs.compare.dao;

import cn.infocore.dbs.compare.model.DbCompare;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface DbCompareDao extends JpaRepository<DbCompare, Long>, PagingAndSortingRepository<DbCompare, Long>, JpaSpecificationExecutor<DbCompare> {
}
