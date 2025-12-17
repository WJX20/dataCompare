package com.crunchydata.services;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;
import com.crunchydata.models.DCConfigurations;


public interface DCConfigurationsService extends IService<DCConfigurations> {
    // 根据ID查询配置
    DCConfigurations getConfigById(Long configId);


    // 分页查询配置
    IPage<DCConfigurations> listConfigsByPage(Integer pageNum, Integer pageSize, String configKey);
}
