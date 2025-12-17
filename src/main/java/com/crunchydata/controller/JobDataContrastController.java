package com.crunchydata.controller;


import com.crunchydata.mapper.DCReconciliateResultMapper;
import com.crunchydata.models.DCReconciliationResult;
import com.crunchydata.result.ReturnT;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * JobDataContrastController.java
 *   数据库对比相关接口
 *
 * 版权所有 (c) 2019-2024, 易景科技保留所有权利。
 * Copyright (c) 2019-2024, Halo Tech Co.,Ltd. All rights reserved.
 *
 * 易景科技是Halo Database、Halo Database Management System、羲和数据
 * 库、羲和数据库管理系统（后面简称 Halo ）软件的发明人同时也为知识产权权
 * 利人。Halo 软件的知识产权，以及与本软件相关的所有信息内容（包括但不限
 * 于文字、图片、音频、视频、图表、界面设计、版面框架、有关数据或电子文档等）
 * 均受中华人民共和国法律法规和相应的国际条约保护，易景科技享有上述知识产
 * 权，但相关权利人依照法律规定应享有的权利除外。未免疑义，本条所指的“知识
 * 产权”是指任何及所有基于 Halo 软件产生的：（a）版权、商标、商号、域名、与
 * 商标和商号相关的商誉、设计和专利；与创新、技术诀窍、商业秘密、保密技术、非
 * 技术信息相关的权利；（b）人身权、掩模作品权、署名权和发表权；以及（c）在
 * 本协议生效之前已存在或此后出现在世界任何地方的其他工业产权、专有权、与“知
 * 识产权”相关的权利，以及上述权利的所有续期和延长，无论此类权利是否已在相
 * 关法域内的相关机构注册。
 *
 * This software and related documentation are provided under a
 * license agreement containing restrictions on use and disclosure
 * and are protected by intellectual property laws. Except as expressly
 * permitted in your license agreement or allowed by law, you may not
 * use, copy, reproduce, translate, broadcast, modify, license, transmit,
 * distribute, exhibit, perform, publish, or display any part, in any
 * form, or by any means. Reverse engineering, disassembly, or
 * decompilation of this software, unless required by law for
 * interoperability, is prohibited.
 *
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including
 * applications that may create a risk of personal injury. If you use
 * this software or in dangerous applications, then you shall be
 * responsible to take all appropriate fail-safe, backup, redundancy,
 * and other measures to ensure its safe use. Halo Tech Corporation and
 * its affiliates disclaim any liability for any damages caused by use
 * of this software in dangerous applications.
 *
 *
 * IDENTIFICATION
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/JobDataContrastController.java
 *
 *-------------------------------------------------------------------------
 */
//@Api(tags = "数据库对比相关接口")
@RestController
@RequestMapping("/api/dataContrast")
public class JobDataContrastController {
//
//    @Autowired
//    private JobDataContrastMapper jobDataContrastMapper;
//
//    @Autowired
//    private DCReconciliateResultMapper dCReconciliateResultMapper;
//
//    @Autowired
//    private DCResultMapper dcResultMapper;
//
//
//    /** 分页查询数据对比列表适应前端新架构 */
//    @GetMapping("/newPageList")
//    public ReturnT <Map<String, Object>> newPageList(int current, int size, String taskName, String metaType) {
//        // page list
//        List<JobDataContrast> list = jobDataContrastMapper.pageList((current - 1) * size, size, taskName, metaType);
//        int recordTotal = jobDataContrastMapper.pageListCount((current - 1) * size, size, taskName, metaType);
//        // package result
//        Map<String, Object> maps = new HashMap<>();
//        maps.put("current", current);
//        maps.put("size", size);
//        maps.put("total", recordTotal);
//        maps.put("records", list);
//        return new ReturnT<>(maps);
//    }
//
//    /**
//     * 数据校验详情列表
//     * @param current
//     * @param size
//     * @param pid
//     * @param tableName
//     * @return
//     */
//    @GetMapping("/pageVerifyList")
//    public ReturnT<Map<String, Object>> pageVerifyList(@RequestParam(required = false, defaultValue = "0") int current,
//                                                       @RequestParam(required = false, defaultValue = "10") int size,
//                                                       int pid, String tableName,
//                                                       @RequestParam(required = false, defaultValue = "all") String status) {
//        // 查询忽略大小写
//        String tableNameIgnoreCae = null;
//        if (tableName != null) {
//            tableNameIgnoreCae = tableName.toLowerCase();
//        }
//        // page list
//        List<DCResult> list = dcResultMapper.pageList((current - 1) * size, size, pid, tableNameIgnoreCae, status);
//        int recordTotal = dcResultMapper.pageListCount((current - 1) * size, size, pid, tableNameIgnoreCae, status);
//        // 计算耗时并赋值
//        list.forEach(dcResult -> {
//            OffsetDateTime start = dcResult.getCompareStart();
//            OffsetDateTime end = dcResult.getCompareEnd();
//            if (start != null && end != null) {
//                Duration duration = Duration.between(start, end);
//                long seconds = duration.getSeconds(); // 总秒数
//                long hours = seconds / 3600;
//                long minutes = (seconds % 3600) / 60;
//                long secs = seconds % 60;
//                // 格式化为 "HH:mm:ss"
//                dcResult.setDurationStr(String.format("%02d:%02d:%02d", hours, minutes, secs));
//            } else {
//                dcResult.setDurationStr("00:00:00");
//            }
//        });
//        // package result
//        Map<String, Object> maps = new HashMap<>();
//        maps.put("current", current);
//        maps.put("size", size);
//        maps.put("total", recordTotal);
//        maps.put("records", list);
//        return new ReturnT<>(maps);
//    }
//
//    /**
//     * 数据校验详情列表
//     * @param current
//     * @param size
//     * @param tid
//     * @return
//     */
//    @GetMapping("/pageVerifyDetailsList")
//    public ReturnT<Map<String, Object>> pageVerifyDetailsList(@RequestParam(required = false, defaultValue = "0") int current,
//                                                              @RequestParam(required = false, defaultValue = "10") int size,
//                                                              int tid) {
//        // page list
//        List<DCReconciliationResult> list = dCReconciliateResultMapper.pageList((current - 1) * size, size, tid);
//        int recordTotal = dCReconciliateResultMapper.pageListCount((current - 1) * size, size, tid);
//        // package result
//        Map<String, Object> maps = new HashMap<>();
//        maps.put("current", current);
//        maps.put("size", size);
//        maps.put("total", recordTotal);
//        maps.put("records", list);
//        return new ReturnT<>(maps);
//    }

}

