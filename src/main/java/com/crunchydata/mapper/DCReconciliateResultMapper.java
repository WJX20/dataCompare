package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.DCReconciliationResult;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DCReconciliateResultMapper extends BaseMapper<DCReconciliationResult> {

    List<DCReconciliationResult> pageList(@Param("offset") int offset,
                                          @Param("pagesize") int pagesize,
                                          @Param("tid") int tid);

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize,
                      @Param("tid") int tid);

    @Delete("delete from dc_reconciliation_results where pid = #{pid}")
    int deleteByPid(@Param("pid") int pid);
}
