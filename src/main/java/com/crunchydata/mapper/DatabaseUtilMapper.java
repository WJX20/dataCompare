package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

/**
 * 数据库工具Mapper接口
 */
@Mapper
public interface DatabaseUtilMapper extends BaseMapper<Object> {

    /**
     * 查询指定schema下的所有UNLOGGED表
     *
     * @param schemaName schema名称
     * @return UNLOGGED表列表
     */
    @Select("SELECT t.table_schema, t.table_name " +
            "FROM information_schema.tables t " +
            "JOIN pg_class c ON c.relname = t.table_name " +
            "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema " +
            "WHERE t.table_schema = #{schemaName} " +
            "AND t.table_type = 'BASE TABLE' " +
            "AND c.relpersistence = 'u'")
    List<Map<String, Object>> findAllUnloggedTables(@Param("schemaName") String schemaName);

    /**
     * 删除指定的表
     *
     * @param schemaName schema名称
     * @param tableName  表名
     */
    @Update("DROP TABLE IF EXISTS ${schemaName}.${tableName} CASCADE")
    void dropTable(@Param("schemaName") String schemaName, @Param("tableName") String tableName);
}
