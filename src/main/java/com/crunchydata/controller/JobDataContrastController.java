package com.crunchydata.controller;


import com.crunchydata.mapper.DCReconciliateResultMapper;
import com.crunchydata.mapper.DCResultMapper;
import com.crunchydata.mapper.JobDataContrastMapper;
import com.crunchydata.models.DCReconciliationResult;
import com.crunchydata.models.DCResult;
import com.crunchydata.models.JobDataContrast;
import com.crunchydata.result.ReturnT;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/api/dataContrast")
public class JobDataContrastController {

    @Autowired
    private JobDataContrastMapper jobDataContrastMapper;

    @Autowired
    private DCReconciliateResultMapper dCReconciliateResultMapper;

    @Autowired
    private DCResultMapper dcResultMapper;


    /** 分页查询数据对比列表适应前端新架构 */
    @GetMapping("/newPageList")
    public ReturnT <Map<String, Object>> newPageList(@RequestParam(required = false, defaultValue = "0") int current,
                                                     @RequestParam(required = false, defaultValue = "10") int size,
                                                     String taskName, String metaType) {
        // page list
        List<JobDataContrast> list = jobDataContrastMapper.pageList((current - 1) * size, size, taskName, metaType);
        int recordTotal = jobDataContrastMapper.pageListCount((current - 1) * size, size, taskName, metaType);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("current", current);
        maps.put("size", size);
        maps.put("total", recordTotal);
        maps.put("records", list);
        return new ReturnT<>(maps);
    }

    /**
     * 数据校验详情列表
     * @param current
     * @param size
     * @param pid
     * @param tableName
     * @return
     */
    @GetMapping("/pageVerifyList")
    public ReturnT<Map<String, Object>> pageVerifyList(@RequestParam(required = false, defaultValue = "0") int current,
                                                       @RequestParam(required = false, defaultValue = "10") int size,
                                                       int pid, String tableName,
                                                       @RequestParam(required = false, defaultValue = "all") String status) {
        // 查询忽略大小写
        String tableNameIgnoreCae = null;
        if (tableName != null) {
            tableNameIgnoreCae = tableName.toLowerCase();
        }
        // page list
        List<DCResult> list = dcResultMapper.pageList((current - 1) * size, size, pid, tableNameIgnoreCae, status);
        int recordTotal = dcResultMapper.pageListCount((current - 1) * size, size, pid, tableNameIgnoreCae, status);
        // 计算耗时并赋值
        list.forEach(dcResult -> {
            OffsetDateTime start = dcResult.getCompareStart();
            OffsetDateTime end = dcResult.getCompareEnd();
            if (start != null && end != null) {
                Duration duration = Duration.between(start, end);
                long seconds = duration.getSeconds(); // 总秒数
                long hours = seconds / 3600;
                long minutes = (seconds % 3600) / 60;
                long secs = seconds % 60;
                // 格式化为 "HH:mm:ss"
                dcResult.setDurationStr(String.format("%02d:%02d:%02d", hours, minutes, secs));
            } else {
                dcResult.setDurationStr("00:00:00");
            }
        });
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("current", current);
        maps.put("size", size);
        maps.put("total", recordTotal);
        maps.put("records", list);
        return new ReturnT<>(maps);
    }

    /**
     * 数据校验详情列表
     * @param current
     * @param size
     * @param tid
     * @return
     */
    @GetMapping("/pageVerifyDetailsList")
    public ReturnT<Map<String, Object>> pageVerifyDetailsList(@RequestParam(required = false, defaultValue = "0") int current,
                                                              @RequestParam(required = false, defaultValue = "10") int size,
                                                              int tid) {
        // page list
        List<DCReconciliationResult> list = dCReconciliateResultMapper.pageList((current - 1) * size, size, tid);
        int recordTotal = dCReconciliateResultMapper.pageListCount((current - 1) * size, size, tid);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("current", current);
        maps.put("size", size);
        maps.put("total", recordTotal);
        maps.put("records", list);
        return new ReturnT<>(maps);
    }

}

