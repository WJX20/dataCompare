package com.crunchydata.controller;


import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.services.JobJdbcDatasourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/jobJdbcDatasource")
public class JobJdbcDatasourceController{
    @Autowired
    private JobJdbcDatasourceService jobJdbcDatasourceService;

    @GetMapping("/datasources")
    public List<JobJdbcDatasource> getDatasources() {
        return jobJdbcDatasourceService.list();
    }

    @GetMapping("/datasourcebyId/{id}")
    public JobJdbcDatasource getDatasourceById(@PathVariable Long id) {
        return jobJdbcDatasourceService.getById(id);
    }

}
