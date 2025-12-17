package com.crunchydata.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.api.R;
import com.crunchydata.models.DCConfigurations;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.DCConfigurationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/configurations")
public class DCConfigurationsController extends BaseController {
    @Autowired
    private DCConfigurationsService dcConfigurationsService;

    @GetMapping("/pageList")
    public ReturnT<IPage<DCConfigurations>> pageList(@RequestParam(value = "configKey", required = false) String configKey,
                                                     @RequestParam("size") Integer size,
                                                     @RequestParam("current") Integer current) {
        return new ReturnT<>(dcConfigurationsService.listConfigsByPage(current, size, configKey));
    }

    // 添加配置
    @PostMapping
    public R<Boolean> insert(@RequestBody DCConfigurations dcConfigurations) {
        return success(this.dcConfigurationsService.save(dcConfigurations));
    }

    // 修改配置
    @PutMapping
    public R<Boolean> update(@RequestBody DCConfigurations dcConfigurations) {
        DCConfigurations dcConfiguration = dcConfigurationsService.getConfigById(dcConfigurations.getConfigId());
        dcConfiguration.setConfigKey(dcConfigurations.getConfigKey());
        dcConfiguration.setConfigType(dcConfigurations.getConfigType());
        dcConfiguration.setConfigValue(dcConfigurations.getConfigValue());
        return success(this.dcConfigurationsService.updateById(dcConfiguration));
    }

    // 根据configId删除配置
    @DeleteMapping("/removeById")
    public R<Boolean> delete(@RequestParam("configId") Long configId) {
        return success(dcConfigurationsService.removeById(configId));
    }
}
