package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.CommonData;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface CommonDataMapper extends BaseMapper<CommonData> {

    List<CommonData> pageList(@Param("offset") int offset,
                              @Param("pagesize") int pagesize,
                              @Param("category") String category);

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize,
                      @Param("category") String category);

    @Select("SELECT content FROM common_data WHERE pid = #{pid} AND category = 'PGCOMPARE_LOG' LIMIT 1")
    String getContentByPidAndCategory(Integer pid);

    @Delete("delete from common_data where pid = #{pid}")
    int deleteByPid(@Param("pid") int pid);

}
