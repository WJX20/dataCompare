package com.crunchydata.services.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.crunchydata.mapper.*;
import com.crunchydata.controller.ColumnController;
import com.crunchydata.controller.ReconcileController;
import com.crunchydata.controller.RepoController;
import com.crunchydata.controller.TableController;
import com.crunchydata.models.*;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.*;
import com.crunchydata.util.Logging;
import com.crunchydata.vo.ReconcileRequestVO;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.crunchydata.controller.TableController.getTableMap;
import static com.crunchydata.services.Reporter.createSection;
import static com.crunchydata.services.Reporter.generateHtmlReport;
import static com.crunchydata.util.Settings.Props;

@Service
public class PgCompareServiceImpl implements PgCompareService {

    private static final String THREAD_NAME = "main";
    private static long startStopWatch;
    private final Map<Integer, List<String>> selectedTablesMap = new ConcurrentHashMap<>();

    @Value("${spring.datasource.url}")
    private String repoUrl;

    @Value("${spring.datasource.username}")
    private String repoUsername;

    @Value("${spring.datasource.password}")
    private String repoPassword;

    @Autowired
    private JobDatasourceMapper jobDataSourceMapper;

    @Autowired
    private DCProjectMapper dcProjectMapper;

    // 删除用到
//    @Autowired
//    private JobDataContrastMapper jobDataContrastMapper;
//
//    @Autowired
//    private DCTableMapper dcTableMapper;

    @Autowired
    private DCReconciliateResultMapper dcReconciliateResultMapper;

//    @Autowired
//    private DCTableHistoryMapper dcTableHistoryMapper;
//
//    @Autowired
//    private DCSourceMapper dcSourceMapper;
//
//    @Autowired
//    private DCTargetMapper dcTargetMapper;
//
//    @Autowired
//    private DCResultMapper dcResultMapper;
    @Autowired
    private CommonDataMapper commonDataMapper;


    public ReturnT<String> performReconciliation(ReconcileRequestVO requestVO, boolean check) {

        Connection connRepo = null;
        Connection connSource = null;
        Connection connTarget = null;
        startStopWatch = System.currentTimeMillis();
        try {
            // 设置 batchNumber 的默认值
            Integer batchNumber = requestVO.getBatchNumber() != null ? requestVO.getBatchNumber() : 0;

            // 提取其他参数
            String projectName = requestVO.getProjectName();
            String reportFileName = requestVO.getProjectName() + "--report";
            String sourceSchema = requestVO.getSourceSchema();
            String targetSchema = requestVO.getTargetSchema();
            // 源端不为Halo
//        List<String> tables = requestVO.getTables();

            // 处理表名，移除模式名前缀（Halo作为源端的时候）
            List<String> tables = requestVO.getTables().stream()
                    .map(table -> {
                        String[] parts = table.split("\\.");
                        return parts[parts.length - 1]; // 总返回最后一个部分
                    })
                    .collect(Collectors.toList());

            // 百分比乘以100
            Double percentage = requestVO.getPercentage() * 100;
            Long sourceId = requestVO.getSourceId();
            Long targetId = requestVO.getTargetId();

            int pid = dcProjectMapper.getByName(projectName);

            JobJdbcDatasource datasourceRepo = new JobJdbcDatasource();
            // 使用 sourceId 和 targetId 查询数据源
            JobJdbcDatasource datasourceSource = jobDataSourceMapper.selectById(sourceId);
            JobJdbcDatasource datasourceTarget = jobDataSourceMapper.selectById(targetId);
            // 配置对应的schema
            Props.setProperty("source-schema", sourceSchema.toLowerCase());
            Props.setProperty("target-schema", targetSchema.toLowerCase());

            if (datasourceSource == null || datasourceTarget == null) {
                Logging.write("severe", THREAD_NAME, "找不到源或目标数据源");
                return new ReturnT<>(101, "找不到源或目标数据源");
            }

            // 设置源和目标的连接属性
            setDatasourceProperties(datasourceRepo, "repo");
            setDatasourceProperties(datasourceSource, "source");
            setDatasourceProperties(datasourceTarget, "target");

            connRepo = dbPostgres.getConnection(Props, "repo", THREAD_NAME);
            if (connRepo == null) {
                Logging.write("severe", THREAD_NAME, "不能连接到存储库");
                Logging.write("config", THREAD_NAME, "Cannot connect to repository database");
                return new ReturnT<>(101, "Cannot connect to repository database");
            }
            // 源端数据库连接
            connSource = getDatabaseConnection(Props.getProperty("source-type"), "source");
            if (connSource == null) {
                Logging.write("severe", THREAD_NAME, "不能连接到源端库");
                Logging.write("config", THREAD_NAME, "Cannot connect to source database");
                return new ReturnT<>(101, "Cannot connect to source database");
            }
            // 目标端数据库连接
            connTarget = getDatabaseConnection(Props.getProperty("target-type"), "target");
            if (connTarget == null) {
                Logging.write("severe", THREAD_NAME, "不能连接到目标端库");
                Logging.write("config", THREAD_NAME, "Cannot connect to target database");
                return new ReturnT<>(101, "Cannot connect to target database");
            }

            // 配置数据库的连接
            if (!check) {

                List<String> selectedTables;
                List<String> allTables = getTableNames(connSource, Props.getProperty("source-schema"), Props.getProperty("source-type"));

                Set<String> allTablesSet = new HashSet<>(allTables);
                Set<String> tablesSet = new HashSet<>(tables);

                boolean isAllTables = tablesSet.equals(allTablesSet);
                boolean isPercentageFull = percentage >= 100;
                // 处理表名或百分比
                if (isPercentageFull || isAllTables) {
                    // 100%和全选
                    selectedTables = tables;
                    saveSelectedTables(batchNumber, selectedTables);
                    TableController.discoverTables(Props, pid, "", connRepo, connSource, connTarget);
                    ColumnController.discoverColumns(Props, pid, "", connRepo, connSource, connTarget);
                } else if (!tables.isEmpty()) {
                    selectedTables = tables;
                    saveSelectedTables(batchNumber, selectedTables);
                    // 发现表和列的逻辑
                    for (String tableName : selectedTables) {
                        TableController.discoverTables(Props, pid, tableName.toLowerCase(), connRepo, connSource, connTarget);
                        ColumnController.discoverColumns(Props, pid, tableName.toLowerCase(), connRepo, connSource, connTarget);
                    }
                } else if (percentage > 0 && percentage < 100) {
                    // 用户指定了百分比，随机选择表
//                List<String> allTables = getTableNames(connSource, Props.getProperty("source-schema"), Props.getProperty("source-type"));
                    if (allTables.isEmpty()) {
                        Logging.write("warning", THREAD_NAME, "源数据库中没有找到表");
                        return new ReturnT<>(101, "源数据库中没有找到表");
                    }
                    int total = allTables.size();
                    int numToSelect = (int) Math.round(percentage / 100.0 * total);
                    if (numToSelect > total) numToSelect = total;

                    selectedTables = new ArrayList<>();
                    Set<String> selectedSet = new HashSet<>(); // 避免重复
                    Random rand = new Random();
                    while (selectedSet.size() < numToSelect) {
                        String selectedTable = allTables.get(rand.nextInt(total));
                        if (selectedSet.add(selectedTable)) {
                            selectedTables.add(selectedTable);
                        }
                    }
                    saveSelectedTables(batchNumber, selectedTables);
                    for (String tableName : selectedTables) {
                        TableController.discoverTables(Props, pid, tableName.toLowerCase(), connRepo, connSource, connTarget);
                        ColumnController.discoverColumns(Props, pid, tableName.toLowerCase(), connRepo, connSource, connTarget);
                    }
                } else {
                    // 默认空处理所有表
                    selectedTables = tables;
                    saveSelectedTables(batchNumber, selectedTables);
                    TableController.discoverTables(Props, pid, "", connRepo, connSource, connTarget);
                    ColumnController.discoverColumns(Props, pid, "", connRepo, connSource, connTarget);
                }
            }

            Logging.write("info", THREAD_NAME, "开始校验: " + batchNumber + " with check: " + check);
            RepoController rpc = new RepoController();
            int tablesProcessed = 0;
            String reTableName;
            List<String> selectedTables = getSelectedTables(batchNumber);
            JSONArray runResult = new JSONArray();
            if (selectedTables == null || selectedTables.isEmpty()) {
                Logging.write("info", THREAD_NAME, "开始执行全量校验");
                reTableName = ""; // 全量校验时获取所有表
                CachedRowSet crsTable = rpc.getTables(pid, connRepo, batchNumber, reTableName, check);
                tablesProcessed = processTables(crsTable, runResult, tablesProcessed, rpc, check, connRepo, connSource, connTarget);
            } else {
                for (String tableName : selectedTables) {
                    String singleReTableName = tableName.toLowerCase();
                    CachedRowSet crsTable = rpc.getTables(pid, connRepo, batchNumber, singleReTableName, check);
                    tablesProcessed = processTables(crsTable, runResult, tablesProcessed, rpc, check,connRepo, connSource, connTarget);
                }
            }
            // TODO 有必要打印吗
//            if (check) {
////            createSummary(tablesProcessed, runResult, startStopWatch, reportFileName);
//                Logging.write("info", THREAD_NAME, "结束校验: " + batchNumber + " with check: " + check);
//            }
            return ReturnT.SUCCESS;
        } catch (Exception e) {
            Logging.write("severe", THREAD_NAME, "数据校验服务执行异常" + e.getMessage());
        } finally {
            // 关闭连接（按逆序关闭）
            try {
                if (connTarget != null) connTarget.close();
            } catch (SQLException e) {
            }
            try {
                if (connSource != null) connSource.close();
            } catch (SQLException e) {
            }
            try {
                if (connRepo != null) connRepo.close();
            } catch (SQLException e) {
            }
        }
        return ReturnT.SUCCESS;
    }


//    @Override
//    @Transactional(rollbackFor = Exception.class)
//    public ReturnT<String> deleteByPid(int pid) {
//
//        jobDataContrastMapper.deleteByPid(pid);
//        dcProjectMapper.deleteByPid(pid);
//        commonDataMapper.deleteByPid(pid);
//
//        // 查询关联的表ID
//        List<Integer> tids = dcTableMapper.selectTidsByPid(pid);
//        if (tids != null && !tids.isEmpty()) {
//            dcTableHistoryMapper.deleteByTids(tids);
//            dcSourceMapper.deleteByTids(tids);
//            dcTargetMapper.deleteByTids(tids);
//            dcResultMapper.deleteByTids(tids);
//        }
//
//        dcTableMapper.deleteByPid(pid);
//        dcReconciliateResultMapper.deleteByPid(pid);
//        return ReturnT.SUCCESS;
//    }


    private int processTables(CachedRowSet crsTable, JSONArray runResult, int tablesProcessed, RepoController rpc, boolean check, Connection connRepo,
                              Connection connSource, Connection connTarget) throws Exception {
        try {
            while (crsTable.next()) {
                tablesProcessed++;
                DCTable dct = new DCTable();
                dct.setPid(crsTable.getInt("pid"));
                dct.setTid(crsTable.getInt("tid"));
                dct.setStatus(crsTable.getString("status"));
                dct.setBatchNbr(crsTable.getInt("batch_nbr"));
                dct.setParallelDegree(crsTable.getInt("parallel_degree"));
                dct.setTableAlias(crsTable.getString("table_alias"));

                Logging.write("info", THREAD_NAME, "开始对表进行校验");

                DCTableMap sourceTableMap = createTableMap("source", dct, connRepo);
                DCTableMap targetTableMap = createTableMap("target", dct, connRepo);

                if (!check) {
                    Logging.write("info", THREAD_NAME, "清除数据对比结果");
                    rpc.deleteDataCompare(connRepo, dct.getTid(), dct.getBatchNbr());
                    rpc.startTableHistory(connRepo, dct.getTid(), "reconcile", dct.getBatchNbr());
                }

                JSONObject actionResult = ReconcileController.reconcileData(Props, connRepo, connSource, connTarget, startStopWatch, check, dct, sourceTableMap, targetTableMap);

                runResult.put(actionResult);

                rpc.completeTableHistory(connRepo, dct.getTid(), "reconcile", dct.getBatchNbr(), 0, actionResult.toString());
            }
            crsTable.close();
        } catch (Exception e) {
//            e.printStackTrace();
            Logging.write("severe", THREAD_NAME, "数据校验异常" + e.getMessage());
//            throw e;
        }
        return tablesProcessed;
    }


    // 设置数据源属性(source 和 target)
    private void setDatasourceProperties(JobJdbcDatasource datasource, String destType) {
        String url;
        if (destType.equals("repo")) {
            url = repoUrl;
        } else {
            url = datasource.getJdbcUrl();
        }
        String host;
        String port = "";
        String dbname = "";
        String repoSchema = "public";
        // 通用解析：去掉JDBC前缀
        String prefix = "jdbc:";
        String dbType;
        String protocol = "";

        // 提取协议部分
        if (url.startsWith(prefix)) {
            url = url.substring(prefix.length());
        }

        // 查找协议分隔符（例如：postgresql://、mysql://等）
        int protocolTypeIndex = url.indexOf(":");
        if (protocolTypeIndex > 0) {
            protocol = url.substring(0, protocolTypeIndex);
        }
        int protocolEndIndex = url.indexOf("//");
        if (protocolEndIndex > 0) {
            // 从"://"之后开始解析
            url = url.substring(protocolEndIndex + 2);
        }

        // 提取repo的type
        if (destType.equals("repo")) {
            dbType = protocol;
        } else {
            dbType = datasource.getDatasource();
            if (dbType.equals("halo") || dbType.equals("postgresql")) {
                dbType = "postgres";
            }
        }
        // 提取host和port
        int colonIndex = url.indexOf(':');
        int slashIndex = url.indexOf('/');

        // 判断URL中是否包含端口和数据库名
        if (colonIndex > 0 && slashIndex > 0) {
            // 有端口和数据库名
            host = url.substring(0, colonIndex); // 主机地址
            port = url.substring(colonIndex + 1, slashIndex); // 端口号
            dbname = url.substring(slashIndex + 1); // 数据库名
        } else if (slashIndex > 0) {
            // 没有端口号，只有主机和数据库名
            host = url.substring(0, slashIndex); // 主机地址
            dbname = url.substring(slashIndex + 1); // 数据库名
        } else {
            // 只有主机，没有数据库名和端口号，默认值处理
            host = url;
        }

        // 如果URL包含查询参数，去掉查询参数部分（例如?ssl=true）
        int questionMarkIndex = dbname.indexOf('?');
        if (questionMarkIndex > 0) {
            String queryString = dbname.substring(questionMarkIndex + 1);
            dbname = dbname.substring(0, questionMarkIndex);
            if (destType.equals("repo") && (dbType.equals("postgresql") && queryString.contains("currentSchema="))
                    || destType.equals("repo") && (dbType.equals("halo") && queryString.contains("currentSchema="))) {
                repoSchema = queryString.split("currentSchema=")[1].split("&")[0];
            }
        }

        if (destType.equals("repo")) {
            Props.setProperty(destType + "-user", repoUsername);
            Props.setProperty(destType + "-password", repoPassword);
            Props.setProperty(destType + "-schema", repoSchema);
        } else {
            // 解密处理
//            Props.setProperty(destType + "-user", AESUtil.decrypt(datasource.getJdbcUsername()));
            Props.setProperty(destType + "-user", datasource.getJdbcUsername());
//            Props.setProperty(destType + "-password", AESUtil.decrypt(datasource.getJdbcPassword()));
            Props.setProperty(destType + "-password", datasource.getJdbcPassword());
            Props.setProperty(destType + "-type", dbType);
            Props.setProperty(destType + "-database-hash", "true");
        }
        Props.setProperty(destType + "-dbname", dbname);
        Props.setProperty(destType + "-host", host);
        Props.setProperty(destType + "-port", port);
//        Props.setProperty(destType + "-schema", repoSchema);
        Props.setProperty(destType + "-sslmode", "disable");

    }

    private DCTableMap createTableMap(String tableOrigin, DCTable dct, Connection connRepo) {
        DCTableMap dctm = getTableMap(connRepo, dct.getTid(), tableOrigin);
        dctm.setBatchNbr(dct.getBatchNbr());
        dctm.setPid(dct.getPid());
        dctm.setTableAlias(dct.getTableAlias());
        return dctm;
    }

    // 辅助方法：获取表名
    private List<String> getTableNames(Connection conn, String schema, String dbType) {
        String query;
        switch (dbType.toLowerCase()) {
            case "halo":
            case "postgres":
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "'";
                break;
            case "mysql":
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "'";
                break;
            case "oracle":
                query = "SELECT table_name FROM all_tables WHERE owner = '" + schema.toUpperCase() + "'";
                break;
            default:
                throw new UnsupportedOperationException("不支持的数据库类型: " + dbType);
        }
        List<String> tableNames = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                tableNames.add(rs.getString("table_name"));
            }
        } catch (SQLException e) {
            Logging.write("severe", THREAD_NAME, "获取表名失败: " + e.getMessage());
            e.printStackTrace();
        }
        return tableNames;
    }

    // 保存发现的表和列
    private void saveSelectedTables(Integer batchNumber, List<String> selectedTables) {
        selectedTablesMap.put(batchNumber, selectedTables);
    }

    // 校验时获取表和列
    private List<String> getSelectedTables(Integer batchNumber) {
        return selectedTablesMap.get(batchNumber);
    }

    private Connection getDatabaseConnection(String dbType, String destType) {
        return switch (dbType) {
            case "oracle" -> dbOracle.getConnection(Props, destType);
            case "mysql" -> dbMySQL.getConnection(Props, destType);
            case "mssql" -> dbMSSQL.getConnection(Props, destType);
            case "db2" -> dbDB2.getConnection(Props, destType);
            case "mariadb" -> dbMariaDB.getConnection(Props, destType);
//            case "halo" -> dbPostgres.getConnection(Props, destType ,"halo---");
//            case "postgres" -> dbPostgres.getConnection(Props, destType ,"pg---");
            default -> dbPostgres.getConnection(Props, destType, THREAD_NAME);
        };
    }

    //
    // Create Column Metadata JSON Object
    //
    private JSONObject createReportColumn(String header, String key, String align, boolean commaFormat) {
        return new JSONObject()
                .put("columnHeader", header)
                .put("columnClass", align)
                .put("columnKey", key)
                .put("commaFormat", commaFormat);
    }
    //
    // Create Summary
    //
//    private void createSummary(int tablesProcessed, JSONArray runResult, long startStopWatch, String reportFileName) {
//        printSummary("Summary: ",0);
//
//        if ( tablesProcessed > 0 ) {
//            long endStopWatch = System.currentTimeMillis();
//            long totalRows = 0;
//            long outOfSyncRows = 0;
//            long elapsedTime = (endStopWatch - startStopWatch) / 1000;
//            // Ensure elapsed time is at least 1 second.
//            elapsedTime = elapsedTime == 0 ? 1 : elapsedTime;
//
//            DecimalFormat df = new DecimalFormat("###,###,###,###,###");
//
//            // Iterate through the runResult array and compute totals
//            for (int i = 0; i < runResult.length(); i++) {
//                JSONObject result = runResult.getJSONObject(i);
//                int nbrEqual = result.getInt("equal");
//                int notEqual = result.getInt("notEqual");
//                int missingSource = result.getInt("missingSource");
//                int missingTarget = result.getInt("missingTarget");
//
//                totalRows += nbrEqual + notEqual + missingSource + missingTarget;
//                outOfSyncRows += notEqual + missingSource + missingTarget;
//
//                // Print per table summary
//                printSummary(String.format("TABLE: %s", result.getString("tableName")), 4);
//                printSummary(String.format("Table Summary: Status         = %s", result.getString("compareStatus")), 8);
//                printSummary(String.format("Table Summary: Equal          = %19d", nbrEqual), 8);
//                printSummary(String.format("Table Summary: Not Equal      = %19d", notEqual), 8);
//                printSummary(String.format("Table Summary: Missing Source = %19d", missingSource), 8);
//                printSummary(String.format("Table Summary: Missing Target = %19d", missingTarget), 8);
//            }
//
//            // Print job summary
//            printSummary("Job Summary: ", 0);
//            printSummary(String.format("Tables Processed               = %s", tablesProcessed), 2);
//            printSummary(String.format("Elapsed Time (seconds)         = %s", df.format(elapsedTime)), 2);
//            printSummary(String.format("Total Rows Processed           = %s", df.format(totalRows)), 2);
//            printSummary(String.format("Total Out-of-Sync              = %s", df.format(outOfSyncRows)), 2);
//            printSummary(String.format("Through-put (rows/per second)  = %s", df.format(totalRows / elapsedTime)), 2);
//
//            // Generate Report
//            // TODO 需要接口传吗
//            boolean genReport = true;
//            if (genReport) {
//                // Create JSON report
//                JSONObject jobSummary = new JSONObject()
//                        .put("tablesProcessed", df.format(tablesProcessed))
//                        .put("elapsedTime", df.format(elapsedTime))
//                        .put("totalRows", df.format(totalRows))
//                        .put("outOfSyncRows", df.format(outOfSyncRows))
//                        .put("rowsPerSecond", df.format(totalRows / elapsedTime));
//
//
//                JSONArray jobSummaryLayout = new JSONArray(List.of(
//                        createReportColumn("Tables Processed", "tablesProcessed", "right-align", false),
//                        createReportColumn("Elapsed Time", "elapsedTime", "right-align", false),
//                        createReportColumn("Rows per Second", "rowsPerSecond", "right-align", false),
//                        createReportColumn("Total Rows", "totalRows", "right-align", false),
//                        createReportColumn("Out of Sync Rows", "outOfSyncRows", "right-align", false)
//                ));
//
//
//                JSONArray runResultLayout = new JSONArray(List.of(
//                        createReportColumn("Table", "tableName", "left-align", false),
//                        createReportColumn("Compare Status", "compareStatus", "left-align", false),
//                        createReportColumn("Elapsed Time", "elapsedTime", "right-align", true),
//                        createReportColumn("Rows per Second", "rowsPerSecond", "right-align", true),
//                        createReportColumn("Rows Total", "totalRows", "right-align", true),
//                        createReportColumn("Rows Equal", "equal", "right-align", true),
//                        createReportColumn("Rows Not Equal", "notEqual", "right-align", true),
//                        createReportColumn("Rows Missing on Source", "missingSource", "right-align", true),
//                        createReportColumn("Rows Missing on Target", "missingTarget", "right-align", true)
//                ));
//
//                JSONArray reportArray = new JSONArray()
//                        .put(createSection("Job Summary", new JSONArray().put(jobSummary), jobSummaryLayout)) // Pass JSONObject directly
//                        .put(createSection("Table Summary", runResult, runResultLayout)); // Pass runResult
//                // TODO 需要接口传吗？
//                if (check) {
//                    JSONArray runCheckResultLayout = new JSONArray(List.of(
//                            createReportColumn("Primary Key", "pk", "left-align", false),
//                            createReportColumn("Status", "compareStatus", "left-align", false),
//                            createReportColumn("Result", "compareResult", "left-align", false)
//                    ));
//
//                    for (int i =0; i<runResult.length(); i++ ) {
//                        reportArray.put(createSection(String.format("Table: %s", runResult.getJSONObject(i).getString("tableName")), runResult.getJSONObject(i).getJSONObject("checkResult").getJSONArray("data"), runCheckResultLayout));
//                    }
//                }
//
//                generateHtmlReport(reportArray, reportFileName,"pgCompare Summary");
//
//            }
//
//        } else {
//            // Print message if no tables processed or out of sync records found
//            String msg = (check) ? "No out of sync records found" : "No tables were processed. Need to do discovery? Used correct batch nbr?";
//            Logging.write("warning", THREAD_NAME, msg);
//        }
//    }

    //
    // Print Summary
    //
    private void printSummary(String message, int indent) {
        Logging.write("info", "summary", " ".repeat(indent) + message);
    }
}
