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
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import com.crunchydata.controller.RepoController;
import com.crunchydata.models.ColumnMetadata;
import com.crunchydata.models.DCTable;
import com.crunchydata.models.DCTableMap;
import com.crunchydata.models.DataCompare;
import com.crunchydata.util.*;
import org.apache.commons.lang3.StringUtils;

import static com.crunchydata.util.HashUtility.getMd5;

/**
 * Thread to pull data from source or target and load into the repository database.
 *
 * @author Brian Pace
 */
public class threadReconcile extends Thread {
    private final Integer tid, batchNbr, cid, nbrColumns, parallelDegree, threadNumber;
    private final String modColumn, pkList, stagingTable, destType;
    private String sql;
    private BlockingQueue<DataCompare[]> q;
    private final ThreadSync ts;
    private final Boolean useDatabaseHash;
    private Properties Props;

    public threadReconcile(Properties Props, Integer threadNumber, DCTable dct, DCTableMap dctm, ColumnMetadata cm, Integer cid, ThreadSync ts, Boolean useDatabaseHash, String stagingTable, BlockingQueue<DataCompare[]> q) {
        this.q = q;
        this.modColumn = dctm.getModColumn();
        this.parallelDegree = dct.getParallelDegree();
        this.sql = dctm.getCompareSQL();
        this.destType = dctm.getDestType();
        this.threadNumber = threadNumber;
        this.nbrColumns = cm.getNbrColumns();
        this.tid = dct.getTid();
        this.cid = cid;
        this.ts = ts;
        this.pkList = cm.getPkList();
        this.useDatabaseHash = useDatabaseHash;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTable = stagingTable;
        this.Props = Props;
    }

    public void run() {

        String threadName = String.format("Reconcile-%s-c%s-t%s", destType, cid, threadNumber);
        Logging.write("info", threadName, String.format("（%s）启动数据库对比线程",destType));
        Logging.write("config", threadName, String.format("(%s) Start database reconcile thread",destType));


        int totalRows = 0;
        int batchCommitSize = Integer.parseInt(Props.getProperty("batch-commit-size"));
        int fetchSize = Integer.parseInt(Props.getProperty("batch-fetch-size"));
        boolean useLoaderThreads = Integer.parseInt(Props.getProperty("loader-threads")) > 0;
        boolean observerThrottle = Boolean.parseBoolean(Props.getProperty("observer-throttle"));
        int cntRecord = 0;
        Connection conn = null;
        boolean firstPass = true;
        DecimalFormat formatter = new DecimalFormat("#,###");
        int loadRowCount = Integer.parseInt(Props.getProperty("batch-progress-report-size"));
        int observerRowCount = Integer.parseInt(Props.getProperty("observer-throttle-size"));
        Connection repoConn = null;
        RepoController rpc = new RepoController();
        ResultSet rs = null;
        PreparedStatement stmt = null;
        PreparedStatement stmtLoad = null;

        try {
            // Connect to Repository
            Logging.write("info", threadName, String.format("(%s) 正在连接存储数据库", destType));
            Logging.write("config", threadName, String.format("(%s) Connecting to repository database", destType));
            repoConn = dbPostgres.getConnection(Props,"repo", "reconcile");

            if ( repoConn == null) {
                Logging.write("severe", threadName, String.format("(%s) 不能连接到存储数据库", destType));
                Logging.write("config", threadName, String.format("(%s) Cannot connect to repository database", destType));
                System.exit(1);
            }
            repoConn.setAutoCommit(false);

            // Connect to Source/Target
            Logging.write("info", threadName, String.format("(%s) 正在连接数据库", destType));
            Logging.write("config", threadName, String.format("(%s) Connecting to database", destType));

            switch (Props.getProperty(destType + "-type")) {
                case "oracle":
                    conn = dbOracle.getConnection(Props,destType);
                    break;
                case "mariadb":
                    conn = dbMariaDB.getConnection(Props,destType);
                    break;
                case "mysql":
                    conn = dbMySQL.getConnection(Props,destType);
                    break;
                case "mssql":
                    conn = dbMSSQL.getConnection(Props,destType);
                    break;
                case "db2":
                    conn = dbDB2.getConnection(Props,destType);
                    break;
                default:
                    conn = dbPostgres.getConnection(Props,destType, "reconcile");
                    conn.setAutoCommit(false);
                    break;
            }

            if ( conn == null) {
                Logging.write("severe", threadName, String.format("(%s) 不能连接到数据库", destType));
                Logging.write("config", threadName, String.format("(%s) Cannot connect to database", destType));
                System.exit(1);
            }

            // Load Reconcile Data
            if ( parallelDegree > 1 && !modColumn.isEmpty()) {
                sql += " AND mod(" + modColumn + "," + parallelDegree +")="+threadNumber;
            }

            if (!pkList.isEmpty() && Props.getProperty("database-sort").equals("true")) {
                sql += " ORDER BY " + pkList;
            }

            // 获取配置值
            String batchCompareSize = Props.getProperty("batch-compare-size");
            String batchOffsetSize = Props.getProperty("batch-offset-size");

            Logging.write("info", threadName, String.format("哈希校验对比总数:  %s", batchCompareSize));
            Logging.write("config", threadName, String.format("Hash Compare Total:  %s", batchCompareSize));
            Logging.write("info", threadName, String.format("哈希对比从第  %s 行开始", batchOffsetSize + 1));
            Logging.write("config", threadName, String.format("Start Comparing From Line %s", batchOffsetSize + 1));

            // oracle 版本需要12以上
            if (StringUtils.isNotEmpty(batchCompareSize) && StringUtils.isNotEmpty(batchOffsetSize)) {
                String dbType = Props.getProperty(destType + "-type"); // 获取目标数据库类型

                // 拼接分页SQL，根据数据库类型区分
                switch (dbType.toLowerCase()) { // 忽略大小写，增强兼容性
                    case "oracle":
                    case "db2":
                    case "mssql":
                        sql += " OFFSET " + batchOffsetSize + " ROWS FETCH NEXT " + batchCompareSize + " ROWS ONLY";
                        break;
                    case "mysql":
                    case "postgres":
                        // PostgreSQL支持两种写法，这里保持与MySQL一致的顺序（PG兼容）
                        sql += " LIMIT " + batchCompareSize + " OFFSET " + batchOffsetSize;
                        break;
                    default:
                        sql += " LIMIT " + batchCompareSize;
                }
            }


            //conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setFetchSize(fetchSize);
            rs = stmt.executeQuery();

            StringBuilder columnValue = new StringBuilder();

            if (!useLoaderThreads) {
                String sqlLoad = "INSERT INTO " + stagingTable + " (tid, pk_hash, column_hash, pk) VALUES (?,?,?,(?)::jsonb)";
                repoConn.setAutoCommit(false);
                stmtLoad = repoConn.prepareStatement(sqlLoad);
            }

            DataCompare[] dc = new DataCompare[batchCommitSize];

            while (rs.next()) {
                columnValue.setLength(0);

                if (! useDatabaseHash) {
                    for (int i = 3; i < nbrColumns + 3; i++) {
                        columnValue.append(rs.getString(i));
                    }
                } else {
                    columnValue.append(rs.getString(3));
                }

                String pkHash = useDatabaseHash ? rs.getString("PK_HASH") : getMd5(rs.getString("PK_HASH"));
                String columnHash = useDatabaseHash ? columnValue.toString() : getMd5(columnValue.toString());

                if (useLoaderThreads) {
                    dc[cntRecord] = new DataCompare(null,tid,null, pkHash, columnHash, rs.getString("PK").replace(",}","}"),null,threadNumber,batchNbr);
                } else {
                    stmtLoad.setInt(1, tid);
                    stmtLoad.setString(2, pkHash);
                    stmtLoad.setString(3, columnHash);
                    stmtLoad.setString(4, rs.getString("PK").replace(",}","}"));
                    stmtLoad.addBatch();
                }

                cntRecord++;
                totalRows++;

                if (totalRows % batchCommitSize == 0 ) {
                    if (useLoaderThreads) {
                        if ( q.size() == 100) {
                            Logging.write("info", threadName, String.format("（%s）正在等待队列空间", destType));
                            Logging.write("config", threadName, String.format("(%s) Waiting for Queue space", destType));
                            while (q.size() > 50) {
                                Thread.sleep(1000);
                            }
                        }
                        q.put(dc);
                        dc = null;
                        dc = new DataCompare[batchCommitSize];
                    } else {
                        stmtLoad.executeLargeBatch();
                        stmtLoad.clearBatch();
                        repoConn.commit();
                    }
                    cntRecord=0;
                }

                if (totalRows % ((firstPass) ? 10000 : loadRowCount) == 0) {
                    Logging.write("info", threadName, String.format("(%s) 加载 %s 行", destType, formatter.format(totalRows)));
                    Logging.write("config", threadName, String.format("(%s) Loaded %s rows", destType, formatter.format(totalRows)));
                }

                if (totalRows % ((firstPass) ? 10000 : observerRowCount) == 0) {
                    if (firstPass || observerThrottle) {
                        firstPass = false;

                        Logging.write("info", threadName, String.format("(%s) 等待观察者", destType));
                        Logging.write("config", threadName, String.format("(%s) Wait for Observer", destType));

                        rpc.dcrUpdateRowCount(repoConn, destType, cid, cntRecord);

                        repoConn.commit();

                        cntRecord=0;

                        if ( destType.equals("source")) {
                            ts.sourceWaiting = true;
                        } else {
                            ts.targetWaiting = true;
                        }

                        ts.observerWait();

                        if ( destType.equals("source")) {
                            ts.sourceWaiting = false;
                        } else {
                            ts.targetWaiting = false;
                        }

                        Logging.write("info", threadName, String.format("（%s）已获观察者确认",destType));
                        Logging.write("config", threadName, String.format("(%s) Cleared by Observer",destType));
                    } else {
                        Logging.write("info", threadName, String.format("(%s) 暂停以供观察者查看",destType));
                        Logging.write("config", threadName, String.format("(%s) Pause for Observer",destType));
                        Thread.sleep(1000);
                    }
                }

            }

            if ( cntRecord > 0 ) {
                if (useLoaderThreads) {
                    q.put(dc);
                } else {
                    stmtLoad.executeBatch();
                }
//                rpc.dcrUpdateRowCount(repoConn, targetType, cid, cntRecord);
            }

            // cntRecord会更新,totalRows
            // totalRows=10000时，触发批次计数更新重置逻辑，这里将cntRecord改为totalRows
            if (totalRows > 0) {
                rpc.dcrUpdateRowCount(repoConn, destType, cid, totalRows);
            }

            Logging.write("info", threadName, String.format("（%s）已完成。总共加载的数据行数：%s", destType, formatter.format(totalRows)));
            Logging.write("config", threadName, String.format("(%s) Complete. Total rows loaded: %s", destType, formatter.format(totalRows)));

            // Wait for Queues to Empty
            if (useLoaderThreads) {
                while (!q.isEmpty()) {
                    Logging.write("info", threadName, String.format("（%s）等待消息队列清空",destType));
                    Logging.write("config", threadName, String.format("(%s) Waiting for message queue to empty",destType));
                    Thread.sleep(1000);
                }
                Thread.sleep(1000);
            }

            if ( destType.equals("source")) {
                ts.sourceComplete = true;
            } else {
                ts.targetComplete = true;
            }

        } catch( SQLException e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", threadName, String.format("（%s）数据库错误发生在第 %s 行：%s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
            Logging.write("config", threadName, String.format("(%s) Database error at line %s:  %s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
            if (ts != null) {
                ts.setException(e);
            }
        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", threadName, String.format("（%s）在第 %s 行的校验线程中出现错误：%s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
            Logging.write("config", threadName, String.format("(%s) Error in reconciliation thread at line %s:  %s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
            if (ts != null) {
                ts.setException(e);
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }

                if (stmtLoad != null) {
                    stmtLoad.close();
                }

                // Close Connections
                if (repoConn != null) {
                    repoConn.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                StackTraceElement[] stackTrace = e.getStackTrace();
                Logging.write("severe", threadName, String.format("（%s）在第 %s 行关闭连接的线程出现错误：%s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
                Logging.write("config", threadName, String.format("(%s) Error closing connections thread at line %s:  %s", destType, stackTrace[0].getLineNumber(), e.getMessage()));
            }
        }

    }

    public ThreadSync getThreadSync() {
        return ts;
    }

}
