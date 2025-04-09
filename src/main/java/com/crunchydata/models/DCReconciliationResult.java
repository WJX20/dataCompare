package com.crunchydata.models;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ibm.db2.cmx.annotation.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("dc_reconciliation_results")
public class DCReconciliationResult {

    @TableId(value = "id",type = IdType.ASSIGN_ID)
    private Long id;                    // 校验结果的唯一标识符
    private Integer pid;                    //项目ID
    private Integer tid;                   // 表ID
    private String tableName;             //表名
    private String pk;                  // 主键，JSON 格式存储
    private String compareStatus;       // 校验状态: in-sync 或 out-of-sync
    private Integer equalCount;         // 相等的记录数量
    private Integer notEqualCount;      // 不相等的记录数量
    private Integer missingSourceCount; // 缺少源数据的记录数量
    private Integer missingTargetCount; // 缺少目标数据的记录数量
    private String resultDetails;   // 校验的差异结果，JSON 格式
    private LocalDateTime createdAt;    // 创建时间
    private LocalDateTime updatedAt;    // 最后更新时间

}

