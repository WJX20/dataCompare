package com.crunchydata.controller;

import com.crunchydata.models.DCConfigurations;
import com.crunchydata.services.DCConfigurationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/configurations")
public class DCConfigurationsController {
    @Autowired
    private DCConfigurationsService dcConfigurationsService;

    // 创建新的属性
    @PostMapping
    public ResponseEntity<DCConfigurations> create(@RequestBody DCConfigurations dcConfigurations) {
        dcConfigurationsService.save(dcConfigurations);
        return ResponseEntity.ok(dcConfigurations);
    }

    // 根据ID查询
    @GetMapping("/{id}")
    public ResponseEntity<DCConfigurations> getById(@PathVariable Long id) {
        DCConfigurations dcConfigurations = dcConfigurationsService.getById(id);
        return ResponseEntity.ok(dcConfigurations);
    }

    // 修改属性
    @PutMapping
    public ResponseEntity<DCConfigurations> update(@RequestBody DCConfigurations dcConfigurations) {
        dcConfigurationsService.updateById(dcConfigurations);
        return ResponseEntity.ok(dcConfigurations);
    }

    // 根据ID删除
    @DeleteMapping("/{id}")
    public ResponseEntity<String> delete(@PathVariable Long id) {
        dcConfigurationsService.removeById(id);
        return ResponseEntity.ok("删除成功");
    }

    // 获取所有属性
    @GetMapping
    public ResponseEntity<List<DCConfigurations>> getAll() {
        List<DCConfigurations> list = dcConfigurationsService.list();
        return ResponseEntity.ok(list);
    }
}
