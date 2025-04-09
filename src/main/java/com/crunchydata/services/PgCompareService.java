package com.crunchydata.services;

import com.crunchydata.vo.ReconcileRequestVO;

public interface PgCompareService {

    void performReconciliation(ReconcileRequestVO requestVO, boolean check);
}
