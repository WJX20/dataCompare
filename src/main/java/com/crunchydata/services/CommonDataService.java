package com.crunchydata.services;

import com.baomidou.mybatisplus.extension.service.IService;
import com.crunchydata.models.CommonData;
import com.crunchydata.result.ReturnT;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

public interface CommonDataService extends IService<CommonData> {


    /**
     * 分页查询
     * @param current
     * @param size
     * @param category
     * @return
     */
    Map<String, Object> pageList(int current, int size, String category);

    /**
     * add
     * 此处采用 Async + REQUIRES_NEW 目的是为了任意情况下都不回滚并且是异步的
     * @param commonData
     * @return
     */
    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    ReturnT<String> add(CommonData commonData);

    /**
     * 查询
     * @param pid
     * @return
     */
    String getContentByPidAndCategory(Integer pid);

}
