package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.DCConfigurationsMapper;
import com.crunchydata.models.DCConfigurations;
import com.crunchydata.services.DCConfigurationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DCConfigurationsServiceImpl extends ServiceImpl<DCConfigurationsMapper, DCConfigurations> implements DCConfigurationsService {
    @Autowired
    private DCConfigurationsMapper dcConfigurationsMapper;

    @Override
    public DCConfigurations getConfigById(Long configId) {
        return getById(configId);
    }

    @Override
    public IPage<DCConfigurations> listConfigsByPage(Integer pageNum, Integer pageSize, String configKey) {
        Page<DCConfigurations> page = new Page<>(pageNum, pageSize);
        return dcConfigurationsMapper.listConfigsByPage(page, configKey);
    }
}
