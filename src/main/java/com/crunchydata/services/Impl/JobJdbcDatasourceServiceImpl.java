package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.JobJdbcDataSourceMapper;
import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.services.JobJdbcDatasourceService;
import org.springframework.stereotype.Service;

@Service
public class JobJdbcDatasourceServiceImpl extends ServiceImpl<JobJdbcDataSourceMapper, JobJdbcDatasource> implements JobJdbcDatasourceService {
}
