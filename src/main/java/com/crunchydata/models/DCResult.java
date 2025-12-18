package com.crunchydata.models;

import lombok.Data;

import java.time.OffsetDateTime;

@Data
public class DCResult {

    private Integer tid;
    private String sourceTableName;
    private String targetTableName;
    private String status;
    private Integer sourceCnt;
    private Integer targetCnt;
    private Integer equalCnt;
    private Integer notEqualCnt;
    private Integer missingSourceCnt;
    private Integer missingTargetCnt;
    private OffsetDateTime compareStart;
    private OffsetDateTime compareEnd;
    // 新增耗时
    private String durationStr;  // 格式化‘耗时’字符串
    // 新增结果信息字段
    private String resultMessage;

}
