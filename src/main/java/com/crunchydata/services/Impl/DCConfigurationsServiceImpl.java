package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.DCConfigerationsMapper;
import com.crunchydata.models.DCConfigurations;
import com.crunchydata.services.DCConfigurationsService;
import org.springframework.stereotype.Service;

@Service
public class DCConfigurationsServiceImpl extends ServiceImpl<DCConfigerationsMapper, DCConfigurations> implements DCConfigurationsService {
}
