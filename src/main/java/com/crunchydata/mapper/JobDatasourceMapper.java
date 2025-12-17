package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.JobJdbcDatasource;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface JobDatasourceMapper extends BaseMapper<JobJdbcDatasource> {
    int update(JobJdbcDatasource datasource);

}
