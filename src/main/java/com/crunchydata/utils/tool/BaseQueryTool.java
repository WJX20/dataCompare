package com.crunchydata.utils.tool;

import cn.hutool.core.util.StrUtil;
import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.result.ReturnT;
import com.crunchydata.utils.JdbcUtils;
import com.crunchydata.utils.meta.DatabaseInterface;
import com.crunchydata.utils.meta.DatabaseMetaFactory;
import com.crunchydata.utils.LocalCacheUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public abstract class BaseQueryTool implements QueryToolInterface {

    //    protected static final Logger logger = LoggerFactory.getLogger(BaseQueryTool.class);
    /**
     * 用于获取查询语句
     */
    private DatabaseInterface sqlBuilder;

    private DataSource datasource;

    private Connection connection;
    /**
     * 当前数据库名
     */
    private String currentSchema;
    private String currentDatabase;

    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    /**
     * 构造方法
     *
     * @param jobDatasource
     */
    BaseQueryTool(JobJdbcDatasource jobDatasource) throws SQLException {
        if (LocalCacheUtil.get(jobDatasource.getDatasourceName()) == null) {
            getDataSource(jobDatasource);
        } else {
            this.connection = (Connection) LocalCacheUtil.get(jobDatasource.getDatasourceName());
            if (!this.connection.isValid(500)) {
                LocalCacheUtil.remove(jobDatasource.getDatasourceName());
                getDataSource(jobDatasource);
            }
        }
        sqlBuilder = DatabaseMetaFactory.getByDbType(jobDatasource.getDatasource());
        currentSchema = getSchema(jobDatasource.getJdbcUsername());
        currentDatabase = jobDatasource.getDatasource();
        LocalCacheUtil.set(jobDatasource.getDatasourceName(), this.connection, 4 * 60 * 60 * 1000);
    }

    private void getDataSource(JobJdbcDatasource jobDatasource) throws SQLException {
        String userName = jobDatasource.getJdbcUsername();

        //这里默认使用 hikari 数据源
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername(userName);
        dataSource.setPassword(jobDatasource.getJdbcPassword());
        dataSource.setJdbcUrl(jobDatasource.getJdbcUrl());
        dataSource.setDriverClassName(jobDatasource.getJdbcDriverClass());
        dataSource.setMaximumPoolSize(1);
        dataSource.setMinimumIdle(0);
        dataSource.setConnectionTimeout(30000);
        this.datasource = dataSource;
//        this.connection = this.datasource.getConnection();
        connectionThreadLocal.set(dataSource.getConnection());
        this.connection = connectionThreadLocal.get();
    }

    //根据connection获取schema
    private String getSchema(String jdbcUsername) {
        String res = null;
        try {
            res = connection.getCatalog();
        } catch (SQLException e) {
            try {
                res = connection.getSchema();
            } catch (SQLException e1) {
//                logger.error("[SQLException getSchema Exception] --> "
//                        + "the exception message is:" + e1.getMessage());
                // 此处借用此异常抛出
                throw new UnsupportedOperationException(e1.getMessage());
            }
//            logger.error("[getSchema Exception] --> "
//                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        }
        // 如果res是null，则将用户名当作 schema
        if (StrUtil.isBlank(res) && StringUtils.isNotBlank(jdbcUsername)) {
            res = jdbcUsername.toUpperCase();
        }
        return res;
    }

    public List<String> getTableSchema() {
        List<String> schemas = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTableSchema();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                schemas.add(tableName);
            }
        } catch (SQLException e) {
//            logger.error("[getTableNames Exception] --> "
//                    + "the exception message is:" + e.getMessage());
            throw new UnsupportedOperationException("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return schemas;
    }

    protected String getSQLQueryTableSchema() {
        return sqlBuilder.getSQLQueryTableSchema();
    }

    @Override
    public List<String> getTableNames(String tableSchema) {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTables(tableSchema);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
            tables.sort(Comparator.naturalOrder());
        } catch (SQLException e) {
//            logger.error("[getTableNames Exception] --> "
//                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    @Override
    public List<String> getTableNames() {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTables();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } catch (SQLException e) {
//            logger.error("[getTableNames Exception] --> "
//                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    protected String getSQLQueryTables(String tableSchema) {
        return sqlBuilder.getSQLQueryTables(tableSchema);
    }

    /**
     * 不需要其他参数的可不重写
     *
     * @return
     */
    protected String getSQLQueryTables() {
        return sqlBuilder.getSQLQueryTables();
    }

    protected Connection getConnection(JobJdbcDatasource jobDatasource, String user,String password) throws SQLException {
        // 从 JobDatasource 中获取连接参数，通过驱动管理器创建连接
        String url = jobDatasource.getJdbcUrl();
        return DriverManager.getConnection(url, user, password);
    }

    public ReturnT<Boolean> dataSourceTest(JobJdbcDatasource jobDatasource, String user, String password) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 1. 建立连接（原逻辑，假设 connection 已通过驱动正确获取）
            connection = getConnection(jobDatasource, user, password);
            if (connection == null || connection.isClosed()) {
//                logger.error("数据库连接未成功建立");
                return new ReturnT<>(ReturnT.FAIL_CODE, "数据库连接未成功建立");
            }

            // 2. 获取数据库类型（用于兼容不同数据库的查询语法）
            DatabaseMetaData metaData = connection.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();

            // 3. 生成通用测试 SQL（兼容主流数据库）
            String testSql;
            if (dbProductName.contains("oracle")) {
                testSql = "SELECT 1 FROM DUAL";  // Oracle 需指定 DUAL 表
            } else if (dbProductName.contains("sqlserver")) {
                // SQL Server 2012+ 支持 VALUES 构造虚拟表；旧版本可用 FROM sys.objects WHERE 1=1
                testSql = "SELECT 1 FROM (VALUES(1)) AS T";
            } else if (dbProductName.contains("gbase") || dbProductName.contains("informix")) {
                // 南大通用GBase 8s、Informix 需指定系统表（如 systables）
                testSql = "SELECT 1 FROM systables WHERE 1=1";
            } else {
                testSql = "SELECT 1";  // MySQL/PostgreSQL/DB2/SQLite/金仓/ClickHouse/Snowflake 等
            }

            // 4. 执行查询验证连接（关键：必须实际执行 SQL 才能验证实例）
            statement = connection.createStatement();
            resultSet = statement.executeQuery(testSql);

            // 5. 检查是否返回结果（只要能执行成功，说明连接到了正确实例）
            return new ReturnT<>(resultSet.next()); // SELECT 1 至少返回 1 行数据

        } catch (SQLException e) {
            String errorMsg = "[数据库连接测试失败] 异常信息: " + e.getMessage();
//            logger.error("[数据库连接测试失败] 异常信息: {}", e.getMessage());
            return new ReturnT<>(ReturnT.FAIL_CODE, errorMsg);
        } finally {
            // 6. 安全关闭所有资源（避免连接泄漏）
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
        }
    }

    // 辅助方法：安全关闭资源（兼容 AutoCloseable）
    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
//                logger.warn("关闭资源失败: {}", e.getMessage());
                // 此处借用此异常抛出
                throw new UnsupportedOperationException(e.getMessage());
            }
        }
    }

}
