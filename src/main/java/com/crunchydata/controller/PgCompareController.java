package com.crunchydata.controller;

import com.baomidou.mybatisplus.extension.api.R;
import com.crunchydata.mapper.DCProjectMapper;
import com.crunchydata.services.DatabaseUtilService;
import com.crunchydata.services.PgCompareService;
import com.crunchydata.util.Logging;
import com.crunchydata.vo.ReconcileRequestVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/pgCompare")
public class PgCompareController {

    @Autowired
    private PgCompareService pgCompareService;

    @Autowired
    public DCProjectMapper dcProjectMapper;

    @Autowired
    private DatabaseUtilService databaseUtilService;


    @PostMapping("/reconcileData")
    @Transactional(rollbackFor = Exception.class)
    public R<String> performReconciliation(@RequestBody ReconcileRequestVO requestVO,boolean check) {

        List<Map<String, String>> droppedTables = databaseUtilService.dropAllUnloggedTables("datax_web");

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", String.format("成功删除 %d 个UNLOGGED表", droppedTables.size()));
        response.put("droppedTables", droppedTables);

        // 先查询数据库中是否已存在该项目名称
        Integer count = dcProjectMapper.countByProjectName(requestVO.getProjectName());
        // 插入projectName
        if (count == 0) {
            dcProjectMapper.saveProject(requestVO.getProjectName());
            System.out.println("项目名称 '" + requestVO.getProjectName() + "' 已插入。");
        } else {
            System.out.println("项目名称 '" + requestVO.getProjectName() + "' 已存在。");
            return R.ok("项目名称 '" + requestVO.getProjectName() + "' 已存在。");
        }

        // 第一步：发现表和列,数据对比映射 (check = false)
        check = false;
        Logging.write("info", "main", "收到校验请求，批次: " + requestVO.getBatchNumber() + ", check: false");
        pgCompareService.performReconciliation(requestVO,check);

        check = true;
        // 第二步：执行数据校验 (check = true)
        Logging.write("info", "main", "收到校验请求，批次: " + requestVO.getBatchNumber() + ", check: true");
        pgCompareService.performReconciliation(requestVO,check);

        return R.ok("校验完成，批次号: " + requestVO.getBatchNumber());
    }

}
