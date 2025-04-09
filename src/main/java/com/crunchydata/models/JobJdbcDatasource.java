package com.crunchydata.models;

import com.baomidou.mybatisplus.annotation.TableName;
import com.ibm.db2.cmx.annotation.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("job_jdbc_datasource")
public class JobJdbcDatasource {
    @Id
    private Long id;
    private String datasourceName;
    private String datasourceGroup;
    private String jdbcUsername;
    private String jdbcPassword;
    private String jdbcUrl;
    private String jdbcDriverClass;
    private Integer status;
    private String createBy;
    private Timestamp createDate;
    private String updateBy;
    private Timestamp updateDate;
    private String comments;
    private String datasource;
    private String zkAdress;
    private String databaseName;
}
