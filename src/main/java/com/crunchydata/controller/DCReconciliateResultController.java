package com.crunchydata.controller;

import com.crunchydata.models.DCReconciliationResult;
import com.crunchydata.services.DCReconciliateResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/reconciliationResult")
public class DCReconciliateResultController {
    @Autowired
    private DCReconciliateResultService dcReconciliateResultService;

    // 根据ID查询
    @GetMapping("/{id}")
    public ResponseEntity<DCReconciliationResult> getById(@PathVariable Long id) {
        DCReconciliationResult dcReconciliationResult = dcReconciliateResultService.getById(id);
        return ResponseEntity.ok(dcReconciliationResult);
    }

    // 根据ID删除
    @DeleteMapping("/{id}")
    public ResponseEntity<String> delete(@PathVariable Long id) {
        dcReconciliateResultService.removeById(id);
        return ResponseEntity.ok("删除成功");
    }

    // 获取所有属性
    @GetMapping
    public ResponseEntity<List<DCReconciliationResult>> getAll() {
        List<DCReconciliationResult> list = dcReconciliateResultService.list();
        return ResponseEntity.ok(list);
    }
}
