package com.crunchydata.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.crunchydata.models.DCTableHistory;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DCTableHistoryMapper extends BaseMapper<DCTableHistory> {

    @Delete("<script>" +
            "DELETE FROM dc_table_history WHERE tid IN " +
            "<foreach collection='tids' item='tid' open='(' separator=',' close=')'>" +
            "#{tid}" +
            "</foreach>" +
            "</script>")
    int deleteByTids(@Param("tids") List<Integer> tids);
}
