package com.crunchydata.services;

import com.crunchydata.result.ReturnT;
import com.crunchydata.vo.ReconcileRequestVO;

public interface PgCompareService {

    /**
     * 创建校验任务
     * @param requestVO
     * @param check
     */
    ReturnT<String> performReconciliation(ReconcileRequestVO requestVO, boolean check) throws Exception;

    /**
     * 删除对比任务
     * @param pid
     * @return
     */
//    ReturnT<String> deleteByPid(int pid);
}
