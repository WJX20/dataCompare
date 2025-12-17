/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crunchydata.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.serial.SerialClob;

import com.crunchydata.dao.ReconciliationResultDAO;
import com.crunchydata.models.*;
import com.crunchydata.util.DataUtility;
import com.crunchydata.util.Logging;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.crunchydata.util.ColumnUtility.createColumnFilterClause;
import static com.crunchydata.util.SQLConstantsRepo.*;

/**
 * Thread to perform reconciliation checks on rows that are out of sync.
 *
 * @author Brian Pace
 */
public class threadReconcileCheck {

    private static final String THREAD_NAME = "ReconcileCheck";

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param ciSource           Column metadata from source database.
     * @param ciTarget           Column metadata from target database.
     * @param cid                Identifier for the reconciliation process.
     */
    public static JSONObject checkRows (Properties Props, Connection repoConn, Connection sourceConn, Connection targetConn, DCTable dct, DCTableMap dctmSource, DCTableMap dctmTarget, ColumnMetadata ciSource, ColumnMetadata ciTarget, Integer cid) {
        ArrayList<Object> binds = new ArrayList<>();
        JSONObject result = new JSONObject();
        JSONArray rows = new JSONArray();

        StringBuilder tableFilter;

        result.put("status","failed");

        try {
            String SQL_REPO_SELECT_OUTOFSYNC_ROWS_LIMIT;
            String batchCheckSize = Props.getProperty("batch-check-size");
            Logging.write("info", THREAD_NAME, String.format("检查差异的数据总数:  %s", batchCheckSize));
            Logging.write("config", THREAD_NAME, String.format("Check the total number of differing data:  %s", batchCheckSize));
            PreparedStatement stmt;
            if (StringUtils.isNotEmpty(batchCheckSize)) {
                SQL_REPO_SELECT_OUTOFSYNC_ROWS_LIMIT = SQL_REPO_SELECT_OUTOFSYNC_ROWS + " LIMIT " + batchCheckSize;
                stmt = repoConn.prepareStatement(SQL_REPO_SELECT_OUTOFSYNC_ROWS_LIMIT);
            } else {
                stmt = repoConn.prepareStatement(SQL_REPO_SELECT_OUTOFSYNC_ROWS);
            }
            stmt.setObject(1, dct.getTid());
            stmt.setObject(2, dct.getTid());
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                DataCompare dcRow = new DataCompare(null,null,null,null,null,null,null, 0, dct.getBatchNbr());
                dcRow.setPid(dct.getPid());
                dcRow.setTid(dct.getTid());
                dcRow.setTableName(dct.getTableAlias());
                dcRow.setPkHash(rs.getString("pk_hash"));
                dcRow.setPk(rs.getString("pk"));
                dcRow.setCompareResult("compare_result");
                int pkColumnCount = 0;
                binds.clear();
                dctmSource.setTableFilter(" ");
                dctmTarget.setTableFilter(" ");
                //tableFilter = new StringBuilder(" AND ");
                JSONObject pk = new JSONObject(dcRow.getPk());
                Iterator<String> keys = pk.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    if (pk.get(key) instanceof String) {
                        String value = pk.getString(key);
                        binds.add(pkColumnCount,value);
                    } else {
                        Integer value = pk.getInt(key);
                        binds.add(pkColumnCount,value);
                    }
                    dctmSource.setTableFilter(dctmSource.getTableFilter() + createColumnFilterClause(repoConn, dct.getTid(), key.toLowerCase(), "source"));
                    dctmTarget.setTableFilter(dctmTarget.getTableFilter() + createColumnFilterClause(repoConn, dct.getTid(), key.toLowerCase(), "target"));
                    pkColumnCount++;
                }
                // 暂时先调整更低的日志等级
                Logging.write("config", THREAD_NAME, String.format("Primary Key:  %s (WHERE = '%s')", pk, dctmSource.getTableFilter().substring(6)));

                JSONObject recheckResult = reCheck(repoConn, sourceConn, targetConn, dctmSource, dctmTarget, ciTarget.pkList, binds, dcRow, cid);

                if ( rows.length() < 1000 ) {
                    rows.put(recheckResult);
                }
            }
//             时间更新一下
            binds.clear();
            binds.add(0, dct.getTid());
            binds.add(1, "reconcile");
            binds.add(2, "reconcile");
            binds.add(3, dct.getBatchNbr());
            dbCommon.simpleUpdate(repoConn, SQL_REPO_DCTABLEHISTORY_END_UPDATE, binds, true);
            result.put("status", "success");

            rs.close();
            stmt.close();
            result.put("data", rows);
            Logging.write("info", THREAD_NAME, String.format("检查对比完成"));
        } catch (Exception e) {
            result.put("status","failed");
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("执行对表“%s”第“%s”行的检查时出现错误：%s", dct.getTableAlias(), stackTrace[0].getLineNumber(), e.getMessage()));
            Logging.write("config", THREAD_NAME, String.format("Error performing check of table %s at line %s:  %s", dct.getTableAlias(), stackTrace[0].getLineNumber(), e.getMessage()));
        }

        return result;
    }

    /**
     * Pulls a list of out-of-sync rows from the repository dc_source and dc_target tables.
     * For each row, calls reCheck where the row is validated against source and target databases.
     *
     * @param repoConn           Repository database connection.
     * @param sourceConn         Source database connection.
     * @param targetConn         Target database connection.
     * @param pkList             Array of primary key columns.
     * @param dcRow              DataCompare object with row to be compared.
     * @param cid                Identifier for the reconciliation process.
     */
    public static JSONObject reCheck(Connection repoConn, Connection sourceConn, Connection targetConn, DCTableMap dctmSource, DCTableMap dctmTarget, String pkList, ArrayList<Object> binds, DataCompare dcRow, Integer cid) {
        JSONArray arr = new JSONArray();
        int columnOutofSync = 0;
        JSONObject rowResult = new JSONObject();

        rowResult.put("compareStatus","in-sync");
        rowResult.put("compareResult"," ");
        rowResult.put("equal",0);
        rowResult.put("notEqual",0);
        rowResult.put("missingSource",0);
        rowResult.put("missingTarget",0);

        CachedRowSet sourceRow = dbCommon.simpleSelect(sourceConn, dctmSource.getCompareSQL() + dctmSource.getTableFilter(), binds);
        CachedRowSet targetRow = dbCommon.simpleSelect(targetConn, dctmTarget.getCompareSQL() + dctmTarget.getTableFilter(), binds);

        try {
            rowResult.put("pk", dcRow.getPk());

            if (sourceRow.size() > 0 && targetRow.size() == 0) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("compareResult", "Missing Target");
                rowResult.put("missingTarget", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Target"));
            } else if (targetRow.size() > 0 && sourceRow.size() == 0 ) {
                rowResult.put("compareStatus", "out-of-sync");
                rowResult.put("compareResult", "Missing Source");
                rowResult.put("missingSource", 1);
                rowResult.put("result", new JSONArray().put(0, "Missing Source"));
            } else {

                RowSetMetaData rowMetadata = (RowSetMetaData) sourceRow.getMetaData();
                sourceRow.next();
                targetRow.next();
                for (int i = 2; i <= rowMetadata.getColumnCount(); i++) {
                    String column = rowMetadata.getColumnName(i);
                    String sourceValue = null;
                    String targetValue = null;

                    try {
                        // 处理Source值，增加空值判断
                        Object sourceObj = sourceRow.getObject(i);
                        if (sourceObj instanceof SerialClob) {
                            sourceValue = DataUtility.convertClobToString((SerialClob) sourceObj);
                        } else {
                            sourceValue = sourceRow.getString(i);
                        }

                        // 处理Target值，增加空值判断
                        Object targetObj = targetRow.getObject(i);
                        if (targetObj instanceof SerialClob) {
                            targetValue = DataUtility.convertClobToString((SerialClob) targetObj);
                        } else {
                            targetValue = targetRow.getString(i);
                        }

                        // 处理可能的null值
                        sourceValue = (sourceValue == null) ? "" : sourceValue;
                        targetValue = (targetValue == null) ? "" : targetValue;


                        if (!sourceValue.equals(targetValue)) {
                            JSONObject col = new JSONObject();
                            // 转义JSON特殊字符
                            String escapedSource = escapeJson(sourceValue);
                            String escapedTarget = escapeJson(targetValue);
                            String jsonString = String.format("{ \"source\": \"%s\", \"target\": \"%s\" }", escapedSource, escapedTarget);
                            col.put(column, new JSONObject(jsonString));
                            arr.put(columnOutofSync, col);
                            columnOutofSync++;
                        }
                    } catch (Exception e) {
                        StackTraceElement[] stackTrace = e.getStackTrace();
                        Logging.write("severe", THREAD_NAME, String.format("在第 %s 行比较列值时出现错误：%s",stackTrace[0].getLineNumber(), e.getMessage()));
                        Logging.write("config", THREAD_NAME, String.format("Error comparing column values at line %s: %s",stackTrace[0].getLineNumber(), e.getMessage()));
                        Logging.write("severe", THREAD_NAME, String.format("错误出现在第 %s 列",column));
                        Logging.write("config", THREAD_NAME, String.format("Error on column %s",column));
                        Logging.write("severe", THREAD_NAME, String.format("源端数据:  %s", sourceRow.getString(i)));
                        Logging.write("config", THREAD_NAME, String.format("Source values:  %s", sourceRow.getString(i)));
                        Logging.write("severe", THREAD_NAME, String.format("目标端数据:  %s", targetRow.getString(i)));
                        Logging.write("config", THREAD_NAME, String.format("Target values:  %s", targetRow.getString(i)));
                    }
                }

                if (columnOutofSync > 0) {
                    rowResult.put("compareStatus", "out-of-sync");
                    rowResult.put("compareResult", arr.toString());
                    rowResult.put("pk", dcRow.getPk());
                    rowResult.put("notEqual", 1);
                    rowResult.put("result", arr);
                }

            }

            if (rowResult.get("compareStatus").equals("in-sync")) {
                rowResult.put("equal",1);
                binds.clear();
                binds.add(0,dcRow.getTid());
                binds.add(1,dcRow.getPkHash());
                binds.add(2, dcRow.getBatchNbr());
                dbCommon.simpleUpdate(repoConn, SQL_REPO_DCSOURCE_DELETE, binds, true);
                dbCommon.simpleUpdate(repoConn, SQL_REPO_DCTARGET_DELETE, binds, true);
            } else {
                // 暂时先调整更低的日志等级
                Logging.write("config", THREAD_NAME, String.format("Out-of-Sync:  PK = %s; Differences = %s", dcRow.getPk(), rowResult.getJSONArray("result").toString()));
            }

            // 创建 ReconciliationResult 对象来保存到数据库
            DCReconciliationResult reconciliationResult = new DCReconciliationResult();
            reconciliationResult.setPid(dcRow.getPid());
            reconciliationResult.setTid(dcRow.getTid());
            reconciliationResult.setTableName(dcRow.getTableName());
            reconciliationResult.setPk(dcRow.getPk());
            reconciliationResult.setCompareStatus(rowResult.getString("compareStatus"));
            reconciliationResult.setEqualCount(rowResult.getInt("equal"));
            reconciliationResult.setNotEqualCount(rowResult.getInt("notEqual"));
            reconciliationResult.setMissingSourceCount(rowResult.getInt("missingSource"));
            reconciliationResult.setMissingTargetCount(rowResult.getInt("missingTarget"));
//            reconciliationResult.setResultDetails(rowResult);
            reconciliationResult.setResultDetails(rowResult.getJSONArray("result").toString());

            // 使用 DAO 类将结果保存到数据库
            ReconciliationResultDAO resultDAO = new ReconciliationResultDAO();
            resultDAO.insertReconciliationResult(repoConn, reconciliationResult);

            // 不生效
//            binds.clear();
//            binds.add(0,rowResult.getInt("equal"));
//            binds.add(1,sourceRow.size());
//            binds.add(2,targetRow.size());
//            binds.add(3,cid);
//            dbCommon.simpleUpdate(repoConn, SQL_REPO_DCRESULT_UPDATE_ALLCOUNTS, binds, true);


        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", THREAD_NAME, String.format("在第 %s 行，源值与目标值的比较出现了错误：%s", stackTrace[0].getLineNumber(), e.getMessage()));
            Logging.write("config", THREAD_NAME, String.format("Error comparing source and target values at line %s:  %s", stackTrace[0].getLineNumber(), e.getMessage()));
        }

        return rowResult;

    }

    /**
     * 转义JSON中的特殊字符
     */
    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

}
