package com.crunchydata.services.Impl;


import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.DatabaseUtilMapper;
import com.crunchydata.services.DatabaseUtilService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库工具服务类
 */
@Service
public class DatabaseUtilServiceImpl extends ServiceImpl<DatabaseUtilMapper, Object> implements DatabaseUtilService {

    @Autowired
    private DatabaseUtilMapper databaseUtilMapper;

    /**
     * 删除指定schema下的所有UNLOGGED表
     *
     * @param schemaName 要清理的schema名称
     * @return 删除的表信息列表
     */
    @Transactional(rollbackFor = Exception.class)
    public List<Map<String, String>> dropAllUnloggedTables(String schemaName) {
        // 1. 查询所有UNLOGGED表
        List<Map<String, Object>> unloggedTables = databaseUtilMapper.findAllUnloggedTables(schemaName);

        List<Map<String, String>> resultList = new ArrayList<>();

        // 2. 删除每个UNLOGGED表
        if (!unloggedTables.isEmpty()) {
            for (Map<String, Object> table : unloggedTables) {
                String tableSchema = (String) table.get("table_schema");
                String tableName = (String) table.get("table_name");

                // 删除表
                databaseUtilMapper.dropTable(tableSchema, tableName);

                // 记录删除信息
                Map<String, String> result = new HashMap<>();
                result.put("schema", tableSchema);
                result.put("table", tableName);
                result.put("status", "deleted");
                resultList.add(result);
            }
        } else {
            System.out.println("上次完整运行没有生成多余的unlogged脏表呢！！！");
        }

        return resultList;
    }
}
