package com.crunchydata.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;

@Data
public class JobDataContrast {

    //@ApiModelProperty("主键ID")
    private int id;

    //@ApiModelProperty("任务名称")
    private String taskName;

    //@ApiModelProperty("源端数据源id")
    private Long readerDatasourceId;

    //@ApiModelProperty("目标端数据源id")
    private Long writerDatasourceId;

    //@ApiModelProperty("源端schema")
    private String readerSchema;

    //@ApiModelProperty("目标端schema")
    private String writerSchema;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date updateTime;

    //@ApiModelProperty("对比类型")
    private String metaType;

    private int pid;

    private String readerDatasourceName;

    private String writerDatasourceName;

}
