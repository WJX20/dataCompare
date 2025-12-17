package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.crunchydata.models.DCConfigurations;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface DCConfigurationsMapper extends BaseMapper<DCConfigurations> {
    /**
     * config page
     * @param page
     * @param configKey
     * @return
     */
    IPage<DCConfigurations> listConfigsByPage(IPage<DCConfigurations> page,
                                              @Param("configKey") String configKey);
}
