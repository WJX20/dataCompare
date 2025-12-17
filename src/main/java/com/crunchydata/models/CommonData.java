package com.crunchydata.models;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
@TableName("common_data")
public class CommonData {

    //    @ApiModelProperty("Id")
    @TableId
    private int id;

    //    @ApiModelProperty("分类")
    private String category;

    //    @ApiModelProperty("内容")
    private String content;

    //    @ApiModelProperty("创建时间")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

    private Integer pid;

}
