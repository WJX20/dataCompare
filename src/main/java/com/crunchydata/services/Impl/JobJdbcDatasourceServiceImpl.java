package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.JobJdbcDataSourceMapper;
import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.JobJdbcDatasourceService;
import com.crunchydata.utils.tool.BaseQueryTool;
import com.crunchydata.utils.tool.QueryToolFactory;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;

@Service
@Transactional(readOnly = true)
public class JobJdbcDatasourceServiceImpl extends ServiceImpl<JobJdbcDataSourceMapper, JobJdbcDatasource> implements JobJdbcDatasourceService {

    @Resource
    private JobJdbcDataSourceMapper datasourceMapper;


    @Override
    public ReturnT<Boolean> dataSourceTest(JobJdbcDatasource jobDatasource) throws IOException {
        // TODO AES 解密逻辑
        String decryptedUser = jobDatasource.getJdbcUsername();
        String decryptedPwd = jobDatasource.getJdbcPassword();
        // 临时使用明文连接数据库（注意：不要修改 jobDatasource 的原始值，避免脏写）
        BaseQueryTool queryTool = QueryToolFactory.getByDbType(jobDatasource);
        return queryTool.dataSourceTest(jobDatasource, decryptedUser, decryptedPwd);
    }

    @Override
    public int update(JobJdbcDatasource datasource) {
        return datasourceMapper.update(datasource);
    }

    @Override
    public List<JobJdbcDatasource> selectAllDatasource() {
        return datasourceMapper.selectList(null);
    }

}
