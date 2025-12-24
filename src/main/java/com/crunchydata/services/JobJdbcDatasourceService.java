package com.crunchydata.services;

import com.baomidou.mybatisplus.extension.service.IService;
import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.result.ReturnT;

import java.io.IOException;
import java.util.List;

public interface JobJdbcDatasourceService extends IService<JobJdbcDatasource> {
    /**
     * 测试数据源
     * @param jdbcDatasource
     * @return
     */
    ReturnT<Boolean> dataSourceTest(JobJdbcDatasource jdbcDatasource) throws IOException;

    /**
     *更新数据源信息
     * @param datasource
     * @return
     */
    int update(JobJdbcDatasource datasource);

    /**
     * 获取所有数据源
     * @return
     */
    List<JobJdbcDatasource> selectAllDatasource();
}
