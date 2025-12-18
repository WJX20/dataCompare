package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.DCTable;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface DCTableMapper extends BaseMapper<DCTable> {

    @Delete("delete from dc_table where pid = #{pid}")
    int deleteByPid(@Param("pid") int pid);

    @Select("select tid from dc_table where pid = #{pid}")
    List<Integer> selectTidListByPid(@Param("pid") int pid);

    // 新增查询tid列表方法
    @Select("SELECT tid FROM dc_table WHERE pid = #{pid}")
    List<Integer> selectTidsByPid(@Param("pid") int pid);
}
