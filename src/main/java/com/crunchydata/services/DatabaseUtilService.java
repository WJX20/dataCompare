package com.crunchydata.services;

import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

public interface DatabaseUtilService extends IService<Object> {

    List<Map<String, String>> dropAllUnloggedTables(String schemaName);
}
