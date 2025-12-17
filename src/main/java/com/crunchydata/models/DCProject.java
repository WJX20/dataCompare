package com.crunchydata.models;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DCProject {
    @TableId(value = "pid",type = IdType.AUTO)
    private Integer pid;
    private String projectName;
    private String projectConfig;
}
