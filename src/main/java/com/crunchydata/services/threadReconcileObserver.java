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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Properties;

import com.crunchydata.controller.RepoController;
import com.crunchydata.models.DCTable;
import com.crunchydata.util.Logging;
import com.crunchydata.util.ThreadSync;

import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_CLEARMATCH;
import static com.crunchydata.util.SQLConstantsRepo.SQL_REPO_DCRESULT_UPDATECNT;

/**
 * Thread class that observes the reconciliation process between source and target tables.
 * This thread executes SQL statements to manage reconciliation and cleanup of staging tables.
 * <p>
 * The observer notifies synchronization threads upon completion of reconciliation steps.
 * </p>
 * <p>
 * Configuration settings include database connection details and SQL statements for reconciliation.
 * </p>
 * <p>
 * This class extends Thread and is designed to run independently for reconciliation monitoring.
 * </p>
 *
 * @author Brian Pace
 */
public class threadReconcileObserver extends Thread  {

    private final Integer tid;
    private final String tableAlias;
    private final Integer cid;
    private final Integer threadNbr;
    private final Integer batchNbr;
    private final String stagingTableSource;
    private final String stagingTableTarget;
    private final ThreadSync ts;
    private Properties Props;
    private final Boolean useLoaderThreads;


    /**
     * Constructs a thread to observe the reconciliation process.
     *
     * @param cid                Identifier for the reconciliation process.
     * @param ts                 Thread synchronization object for coordinating threads.
     * @param threadNbr          Thread number identifier.
     * @param stagingTableSource Staging table name for the source data.
     * @param stagingTableTarget Staging table name for the target data.
     *
     * @author Brian Pace
     */
    public threadReconcileObserver(Properties Props, DCTable dct, Integer cid, ThreadSync ts, Integer threadNbr, String stagingTableSource, String stagingTableTarget) {
        this.tid = dct.getTid();
        this.tableAlias = dct.getTableAlias();
        this.cid = cid;
        this.ts = ts;
        this.threadNbr = threadNbr;
        this.batchNbr = dct.getBatchNbr();
        this.stagingTableSource = stagingTableSource;
        this.stagingTableTarget = stagingTableTarget;
        this.Props = Props;
        this.useLoaderThreads =  (Integer.parseInt(Props.getProperty("loader-threads")) > 0);
    }

    /**
     * Executes the reconciliation observer thread.
     * This method manages database connections, executes SQL statements for reconciliation,
     * and performs cleanup operations on staging tables.
     */
    public void run() {
        String threadName = String.format("Observer-c%s-t%s", cid, threadNbr);
        Logging.write("info", threadName, "启动比对观察者线程");
        Logging.write("config", threadName, "Starting reconcile observer");

        ArrayList<Object> binds = new ArrayList<>();
        int cntEqual = 0;
        int deltaCount = 0;
        int loaderThreads = Integer.parseInt(Props.getProperty("loader-threads"));
        DecimalFormat formatter = new DecimalFormat("#,###");
        int lastRun = 0;
        RepoController rpc = new RepoController();
        int sleepTime = 1000;

        // Connect to Repository
        Logging.write("info", threadName, "正在连接存储库...");
        Logging.write("config", threadName, "Connecting to repository database");
        Connection repoConn = dbPostgres.getConnection(Props,"repo", "observer");

        if ( repoConn == null) {
            Logging.write("severe", threadName, "无法连接到存储库数据库");
            Logging.write("config", threadName, "Cannot connect to repository database");
            System.exit(1);
        }

        try { repoConn.setAutoCommit(false); } catch (Exception e) {
            // do nothing
        }

        try {
            dbCommon.simpleExecute(repoConn,"set enable_nestloop='off'");
            dbCommon.simpleExecute(repoConn,"set work_mem='512MB'");
            dbCommon.simpleExecute(repoConn,"set maintenance_work_mem='1024MB'");
        } catch (Exception e) {
            // do nothing
        }

        // Watch Reconcile Loop
        try {
            String sqlClearMatch = SQL_REPO_CLEARMATCH.replaceAll("dc_target",stagingTableTarget).replaceAll("dc_source",stagingTableSource);

            PreparedStatement stmtSU = repoConn.prepareStatement(sqlClearMatch);
            PreparedStatement stmtSUS = repoConn.prepareStatement(SQL_REPO_DCRESULT_UPDATECNT);

            repoConn.setAutoCommit(false);

            int tmpRowCount;

            while (lastRun <= 1) {
                // 优先检查异常：若 Reconcile 线程已设置异常，立即退出循环
                if (ts.exceptionSet) {
                    Logging.write("severe", threadName, "校验线程检测到异常，正在退出观察.");
                    Logging.write("config", threadName, "Detected exception from reconcile thread, exiting observer.");
                    break; // 退出循环，不再等待正常完成信号
                }
                // Remove Matching Rows
                tmpRowCount = stmtSU.executeUpdate();

                cntEqual += tmpRowCount;

                if (tmpRowCount > 0) {
                    repoConn.commit();
                    deltaCount += tmpRowCount;
                    Logging.write("info", threadName, String.format("匹配了 %s 行数据", formatter.format(tmpRowCount)));
                    Logging.write("config", threadName, String.format("Matched %s rows", formatter.format(tmpRowCount)));
                } else {
                    if (cntEqual > 0 || ts.sourceComplete || ts.targetComplete || ( cntEqual == 0 && ts.sourceWaiting && ts.targetWaiting ) ) {
                        stmtSUS.clearParameters();
                        stmtSUS.setInt(1,deltaCount);
                        stmtSUS.setInt(2,cid);
                        stmtSUS.executeUpdate();
                        repoConn.commit();
                        deltaCount=0;
                        ts.observerNotify();
                        if ( Boolean.parseBoolean(Props.getProperty("observer-vacuum")) ) {
                            repoConn.setAutoCommit(true);
                            binds.clear();
                            dbCommon.simpleUpdate(repoConn, String.format("vacuum %s,%s", stagingTableSource, stagingTableTarget), binds, false);
                            repoConn.setAutoCommit(false);
                        }
                    }
                }

                // Update and Check Status
                if ( ts.sourceComplete && ts.targetComplete && tmpRowCount == 0 && (ts.loaderThreadComplete == loaderThreads*2 || ! useLoaderThreads) ) {
                    lastRun++;
                }

                if ( tmpRowCount == 0 ) {
                    if (Props.getProperty("database-sort").equals("false") && cntEqual == 0) { ts.observerNotify(); }
//                    Thread.sleep(sleepTime);
                    // 关键：用 wait 替代 sleep，响应 setException() 的 notifyAll() 唤醒
                    synchronized (ts) { // 需在同步块中调用 wait
                        try {
                            ts.wait(sleepTime); // 超时等待 1 秒，避免永久阻塞
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break; // 中断时退出
                        }
                    }
                } else {
                    // Standard Sleep
                    if ( cntEqual > 500000 ) {
                        Thread.sleep(500);
                    }
                }
            }

            // 循环退出后处理异常
            if (ts.exceptionSet) {
                Exception ex = ts.getAndClearException();
                Logging.write("severe", threadName, "观察因校验错误而终止: " + ex.getMessage());
                Logging.write("config", threadName, "Observer aborted due to reconcile error: " + ex.getMessage());
                if (ts != null) {
                    ts.setException(ex);
                }
            }

            stmtSUS.close();
            stmtSU.close();

            Logging.write("info", threadName, "清理临时表");
            Logging.write("config", threadName, "Staging table cleanup");

            // Move Out-of-Sync rows from temporary staging tables to dc_source and dc_target
            rpc.loadFindings(repoConn, "source", tid, tableAlias, stagingTableSource, batchNbr, threadNbr);
            rpc.loadFindings(repoConn, "target", tid, tableAlias, stagingTableTarget, batchNbr, threadNbr);

            // Drop staging tables
            rpc.dropStagingTable(repoConn, stagingTableSource);
            rpc.dropStagingTable(repoConn, stagingTableTarget);


        } catch (Exception e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            Logging.write("severe", threadName, String.format("观察者进程在第 %s 行出现错误：%s", stackTrace[0].getLineNumber(), e.getMessage()));
            Logging.write("config", threadName, String.format("Error in observer process at line %s: %s", stackTrace[0].getLineNumber(), e.getMessage()));
            try { repoConn.rollback();
            } catch (Exception ee) {
                stackTrace = e.getStackTrace();
                Logging.write("warning", threadName, String.format("在第 %s 行回滚事务时出现错误：%s ",stackTrace[0].getLineNumber(), e.getMessage()));
                Logging.write("config", threadName, String.format("Error rolling back transaction at line %s: %s ",stackTrace[0].getLineNumber(), e.getMessage()));
            }
            if (ts != null) {
                ts.setException(e);
            }
        } finally {
            try {
                repoConn.close();
            } catch (Exception e) {
                StackTraceElement[] stackTrace = e.getStackTrace();
                Logging.write("warning", threadName, String.format("在第 %s 行关闭线程时出现错误：%s",stackTrace[0].getLineNumber(), e.getMessage()));
                Logging.write("config", threadName, String.format("Error closing thread at line %s:  %s",stackTrace[0].getLineNumber(), e.getMessage()));
                if (ts != null) {
                    ts.setException(e);
                }
            }
        }
    }

    // 添加获取ThreadSync方法，用于获取异常
    public ThreadSync getThreadSync() {
        return ts;
    }

}
