package com.crunchydata.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReconcileRequestVO {
    private Integer batchNumber;    // 批次号
    private Integer pid;
    private String projectName;
    private boolean check;
    private String sourceSchema;    // 源模式
    private String targetSchema;    // 目标模式
    private List<String> tables;    // 表列表
    private Double percentage;      // 校验百分比
    private Integer sourceId;       // 源 ID
    private Integer targetId;       // 目标 ID
}
