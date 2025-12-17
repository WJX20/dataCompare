package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.DCProject;
import org.apache.ibatis.annotations.*;

@Mapper
public interface DCProjectMapper extends BaseMapper<DCProject> {

    @Select("select pid from dc_project where project_name = #{projectName}")
    int getByName(@Param("projectName") String projectName);

    @Insert("insert into dc_project (project_name) values (#{projectName})")
    void saveProject(@Param("projectName") String projectName);

    // 查询是否存在该项目名称
    @Select("select count(*) from dc_project where project_name = #{projectName}")
    Integer countByProjectName(@Param("projectName") String projectName);

    @Delete("delete from dc_project where pid = #{pid}")
    int deleteByPid(@Param("pid") int pid);
}
