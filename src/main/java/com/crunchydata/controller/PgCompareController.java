package com.crunchydata.controller;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.crunchydata.mapper.DCProjectMapper;
import com.crunchydata.mapper.JobDataContrastMapper;
import com.crunchydata.models.DCConfigurations;
import com.crunchydata.models.JobDataContrast;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.CommonDataService;
import com.crunchydata.services.DCConfigurationsService;
import com.crunchydata.services.DatabaseUtilService;
import com.crunchydata.services.PgCompareService;
import com.crunchydata.util.Logging;
import com.crunchydata.vo.ReconcileRequestVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.crunchydata.util.Settings.Props;


@RestController
@RequestMapping("/api/pgCompare")
public class PgCompareController {


    @Autowired
    private PgCompareService pgCompareService;

    @Autowired
    public DCProjectMapper dcProjectMapper;

    @Autowired
    private JobDataContrastMapper jobDataContrastMapper;

    @Autowired
    private DatabaseUtilService databaseUtilService;

    @Autowired
    private CommonDataService commonDataService;

//    @Autowired
//    private SubMetaDataService subMetaDataService;

    @Autowired
    private DCConfigurationsService dcConfigurationsService;

    @PostMapping("/reconcileData")
    public ReturnT<String> performReconciliation(@RequestBody(required = false) ReconcileRequestVO requestVO) {
        try {

//            // 1. 使用 CrossPlatformPathUtils 获取统一的 meta 基础目录
//            String metaBaseDir = CrossPlatformPathUtils.getMetaBaseDir(this.getClass());
//
//            // 2. 定义日志子目录（meta/log/）
//            String logDir = metaBaseDir + "log" + File.separator;
//
//            // 3. 确保日志目录存在（自动创建不存在的目录）
//            CrossPlatformPathUtils.createDirIfNotExists(logDir);
//
//            // 4. 生成日志文件名（处理特殊字符，避免跨平台命名问题）
//            String originalLogName = requestVO.getLogFileName();
//            // 过滤文件名中的特殊字符（适配Windows/Linux）
//            String safeLogFileName = CrossPlatformPathUtils.sanitizeFileName(originalLogName + ".log");
//
//            // 5. 拼接完整日志文件路径
//            String logFilePath = logDir + safeLogFileName;
//
//            // 6. 设置线程日志文件
//            Logging.setThreadLogFile(logFilePath);

            // 获取system的默认参数
            QueryWrapper<DCConfigurations> wrapper = new QueryWrapper<>();
            wrapper.eq("config_type", "system").orderByAsc("config_id");
            List<DCConfigurations> systemProperties = dcConfigurationsService.list(wrapper);
            for (DCConfigurations dcConfigurations : systemProperties) {
                Props.setProperty(dcConfigurations.getConfigKey(), dcConfigurations.getConfigValue());
            }
            // 初始化日志等级
            Logging.initialize(Props);

            List<Map<String, String>> droppedTables = databaseUtilService.dropAllUnloggedTables("pgcompare");

            // 先查询数据库中是否已存在该项目名称
            Integer count = dcProjectMapper.countByProjectName(requestVO.getProjectName());
            // 插入projectName
            if (count == 0) {
                dcProjectMapper.saveProject(requestVO.getProjectName());
                Logging.write("info", "main", "项目名称 '" + requestVO.getProjectName() + "' 已插入。");
            } else {
                Logging.write("severe", "main", "项目名称 '" + requestVO.getProjectName() + "' 已存在。");
                return new ReturnT<>(101, "项目名称 '" + requestVO.getProjectName() + "' 已存在。");
            }

            int pid = dcProjectMapper.getByName(requestVO.getProjectName());
            Logging.write("info", "main", "这是pid:------------------" + pid);

            // 将日志路径持久化到数据库中
//            commonDataService.add(CommonData.builder().category("PGCOMPARE_LOG").content(logFilePath).pid(pid).build());

            // 存储在原来表中数据详情(主要是需要源和schema);
            JobDataContrast jobDataContrast = new JobDataContrast();
            jobDataContrast.setMetaType("1");
            jobDataContrast.setPid(pid);
            jobDataContrast.setTaskName(requestVO.getProjectName());
            jobDataContrast.setReaderDatasourceId(requestVO.getSourceId());
            jobDataContrast.setWriterDatasourceId(requestVO.getTargetId());
            jobDataContrast.setReaderSchema(requestVO.getSourceSchema());
            jobDataContrast.setWriterSchema(requestVO.getTargetSchema());
            jobDataContrastMapper.save(jobDataContrast);
            // 第一步：发现表和列,数据对比映射 (check = false)
            boolean check = false;
            Logging.write("info", "main", "收到校验请求，批次: " + requestVO.getBatchNumber() + ", check: false");
            pgCompareService.performReconciliation(requestVO, check);

            check = true;
            // 第二步：执行数据校验 (check = true)
            Logging.write("info", "main", "收到校验请求，批次: " + requestVO.getBatchNumber() + ", check: true");
            pgCompareService.performReconciliation(requestVO, check);

            Logging.write("info", "main", "校验完成，批次号: " + requestVO.getBatchNumber());
            return new ReturnT<>(200, "校验完成，批次号: " + requestVO.getBatchNumber());
        } catch (Exception e) {
            // 捕获路径异常或业务异常，记录日志并返回错误信息
            String errorMsg = "校验失败: " + e.getMessage();
            // 若日志未初始化成功，直接打印到控制台
            try {
                Logging.write("severe", "main", errorMsg);
            } catch (Exception logEx) {
                // 日志写入失败时的降级处理
                System.err.println("日志记录失败: " + logEx.getMessage() + "，原错误: " + errorMsg);
            }
            return new ReturnT<>(101, errorMsg);
//        } finally {
//            Logging.closeThreadLogFile();
        }
    }

//    @RequestMapping(value = "/removePid", method = RequestMethod.POST)
//    public ReturnT<String> removePid(int pid) {
//        return pgCompareService.deleteByPid(pid);
//    }

//    @GetMapping("/compareLogDetail")
//    public ReturnT<Map<String, Object>> getCompareLogDetail(@RequestParam Integer pid) {
//        String content = commonDataService.getContentByPidAndCategory(pid);
//
//        if (content == null || content.trim().isEmpty()) {
//            return new ReturnT<>(ReturnT.FAIL_CODE, "日志内容为空");
//        }
//
//        String fileName = Paths.get(content).getFileName().toString();
//        String logFileName = FilenameUtils.removeExtension(fileName);
//
//        Map<String, Object> result = subMetaDataService.logDetail(logFileName);
//
//        if (Boolean.TRUE.equals(result.get("success"))) {
//            // 成功时，直接返回content
//            return new ReturnT<>(result);
//        } else {
//            // 失败时，提取error信息到msg字段，保留content中的其他信息
//            String errorMsg = (String) result.get("error");
//            ReturnT<Map<String, Object>> returnT = new ReturnT<>(ReturnT.FAIL_CODE, errorMsg);
//            returnT.setContent(result);
//            return returnT;
//        }
//    }

}

