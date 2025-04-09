package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.crunchydata.mapper.DCReconciliateResultMapper;
import com.crunchydata.models.DCReconciliationResult;
import com.crunchydata.services.DCReconciliateResultService;
import org.springframework.stereotype.Service;

@Service
public class DCReconciliateResultServiceImpl extends ServiceImpl<DCReconciliateResultMapper, DCReconciliationResult> implements DCReconciliateResultService {
}
