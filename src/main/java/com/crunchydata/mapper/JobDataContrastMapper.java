package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.JobDataContrast;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface JobDataContrastMapper extends BaseMapper<JobDataContrast> {

    int save(JobDataContrast jobDataContrast);

    int update(JobDataContrast jobDataContrast);

    List<JobDataContrast> pageList(@Param("offset") int offset,
                                   @Param("pagesize") int pagesize,
                                   @Param("taskName") String taskName,
                                   @Param("metaType") String metaType);

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize,
                      @Param("taskName") String taskName,
                      @Param("metaType") String metaType);

    int delete(@Param("id") int id);

    @Delete("delete from job_data_contrast where pid = #{pid}")
    int deleteByPid(@Param("pid") int pid);

    JobDataContrast getInfoById(@Param("id") int id);

}
