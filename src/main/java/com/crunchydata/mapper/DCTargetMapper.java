package com.crunchydata.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DCTargetMapper {
    @Delete("<script>" +
            "DELETE FROM dc_target WHERE tid IN " +
            "<foreach collection='tids' item='tid' open='(' separator=',' close=')'>" +
            "#{tid}" +
            "</foreach>" +
            "</script>")
    int deleteByTids(@Param("tids") List<Integer> tids);
}
