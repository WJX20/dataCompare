package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.CommonDataMapper;
import com.crunchydata.models.CommonData;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.CommonDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("commonDataService")
public class CommonDataServiceImpl extends ServiceImpl<CommonDataMapper, CommonData> implements CommonDataService {


    @Autowired
    private CommonDataMapper commonDataMapper;

    @Override
    public Map<String, Object> pageList(int current, int size, String category) {
        // page list
        List<CommonData> list = commonDataMapper.pageList((current - 1) * size, size, category);
        int list_count = commonDataMapper.pageListCount((current - 1) * size, size, category);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("current", current);
        maps.put("size", size);
        maps.put("total", list_count);
        maps.put("records", list);
        return maps;
    }

    @Override
    public ReturnT<String> add(CommonData commonData) {
        this.save(commonData);
        return ReturnT.SUCCESS;
    }

    @Override
    public String getContentByPidAndCategory(Integer pid) {
        return commonDataMapper.getContentByPidAndCategory(pid);
    }
}
