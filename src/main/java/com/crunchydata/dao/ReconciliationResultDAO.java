package com.crunchydata.dao;

import com.crunchydata.models.DCReconciliationResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ReconciliationResultDAO {

    /**
     * 将校验结果插入数据库
     */
    public void insertReconciliationResult(Connection conn, DCReconciliationResult result) throws SQLException {
        String sql = "INSERT INTO dc_reconciliation_results (pid, tid, table_name, pk, compare_status, equal_count, not_equal_count, " +
                "missing_source_count, missing_target_count, result_details, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, result.getPid());
            stmt.setInt(2, result.getTid());
            stmt.setString(3, result.getTableName());
            stmt.setString(4, result.getPk());
            stmt.setString(5, result.getCompareStatus());
            stmt.setInt(6, result.getEqualCount());
            stmt.setInt(7, result.getNotEqualCount());
            stmt.setInt(8, result.getMissingSourceCount());
            stmt.setInt(9, result.getMissingTargetCount());
            stmt.setString(10, result.getResultDetails().toString());
            stmt.executeUpdate();
        }
    }
}
