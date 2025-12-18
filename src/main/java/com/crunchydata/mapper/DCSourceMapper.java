package com.crunchydata.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DCSourceMapper {

    @Delete("<script>" +
            "DELETE FROM dc_source WHERE tid IN " +
            "<foreach collection='tids' item='tid' open='(' separator=',' close=')'>" +
            "#{tid}" +
            "</foreach>" +
            "</script>")
    int deleteByTids(@Param("tids") List<Integer> tids);
}
