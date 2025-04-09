package com.crunchydata.models;


import com.baomidou.mybatisplus.annotation.TableField;
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
@TableName("dc_configurations")
public class DCConfigurations {
    @TableId(value = "config_id")
    private Long configId;
    private String configKey;
    private String configValue;
    @TableField("config_type")
    private String configType;
    private Long project_id;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
