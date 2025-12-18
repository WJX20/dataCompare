package com.crunchydata.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.DCResult;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DCResultMapper extends BaseMapper<DCResult> {

    List<DCResult> pageList(@Param("offset") int offset,
                            @Param("pagesize") int pagesize,
                            @Param("pid") int pid,
                            @Param("tableName") String tableName,
                            @Param("status") String status);

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize,
                      @Param("pid") int pid,
                      @Param("tableName") String tableName,
                      @Param("status") String status);

    @Delete("<script>" +
            "DELETE FROM dc_result WHERE tid IN " +
            "<foreach collection='tids' item='tid' open='(' separator=',' close=')'>" +
            "#{tid}" +
            "</foreach>" +
            "</script>")
    int deleteByTids(@Param("tids") List<Integer> tids);
}
