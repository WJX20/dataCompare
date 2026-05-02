<!--@nrg.languages=en,zh-->
<!--@nrg.defaultLanguage=en-->
<!--@nrg.fileNamePattern.zh=README_zh.md-->

<div>
  <h1 style="font-size: 70px;text-align: center">data compare</h1><!--en-->
  <h1 style="font-size: 70px;text-align: center">数据对比</h1><!--zh-->
  <h2 style="text-align: center">Data Compare</h2>
  <h2 style="text-align: center">Repository based on PostgreSQL</h2><!--en-->
  <h2 style="text-align: center">存储库基于PostgreSQL</h2><!--zh-->
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple<!--en-->
# 简单易用的数据对比工具<!--zh-->

**Data Compare** is a Java-based tool for validating data consistency after replication or migration between databases. It's designed for scenarios like:<!--en-->
**数据对比（Data Compare）** 是一个基于Java的工具，用于验证数据库之间复制或迁移后的数据一致性。它适用于以下场景：<!--zh-->

- **Data migration from Oracle/DB2/MariaDB/MySQL/MSSQL to Postgres:** Compare data post-migration.<!--en-->
- **从Oracle/DB2/MariaDB/MySQL/MSSQL到PostgreSQL的数据迁移：** 迁移后比较数据。<!--zh-->
<!--en-->
- **相同或不同数据库平台之间的逻辑复制：** 在最小化数据库开销的同时跨平台验证数据。<!--zh-->
- **Logical replication between same or different database platforms:** Validate data across platforms while minimizing database overhead.<!--en-->
- **主-主复制配置：** 定期验证数据一致性以降低风险。<!--zh-->

- **Active-Active replication configuration:** Regularly verify data consistency to mitigate risks.<!--en-->
DataCompare使用哈希算法高效地比较表数据。主键和其余列的哈希值存储在仓库中，减少存储和网络需求。比较过程是并行处理的，提高性能。<!--zh-->

DataCompare uses hashing to compare table data efficiently. Hash values for primary keys and remaining columns are stored in a repository, reducing storage and network demands. Comparisons are processed in parallel, improving performance.<!--en-->
# 特性<!--zh-->

# Features<!--en-->
- 支持Oracle、PostgreSQL、DB2、MariaDB、MySQL和MSSQL。<!--zh-->
<!--en-->
- 使用哈希进行高效的并行比较。<!--zh-->
- Supports Oracle, PostgreSQL, DB2, MariaDB, MySQL, and MSSQL.<!--en-->
- 处理批处理以进行性能调优。<!--zh-->
- Efficient parallel comparisons using hashing.<!--en-->
- 在中央仓库中存储多个比较项目的配置。<!--zh-->
- Handles batch processing for performance tuning.<!--en-->
<!--zh-->
- Stores configurations for multiple comparison projects in a central repository.<!--en-->
# 安装<!--zh-->

# Installation<!--en-->
## 要求<!--zh-->

## Requirements<!--en-->
在开始构建和安装过程之前，请确保满足以下先决条件：<!--zh-->

Before initiating the build and installation process, ensure the following prerequisites are met:<!--en-->
1. **Java** 17 或更高版本。<!--zh-->
<!--en-->
2. **Maven** 3.8 或更高版本。<!--zh-->
1. **Java** 17 or later.<!--en-->
3. **PostgreSQL** 15 或更高版本（用于仓库）。<!--zh-->
2. **Maven** 3.8 or later.<!--en-->
4. 支持的JDBC驱动程序（当前支持DB2、PostgreSQL、MySQL、MSSQL、MariaDB和Oracle）。<!--zh-->
3. **PostgreSQL** 15 or later (for the repository).<!--en-->
5. 直接PostgreSQL连接。<!--zh-->
4. Supported JDBC drivers (DB2, Postgres, MySQL, MSSQL and Oracle currently supported).<!--en-->
6. Vue3、pnpm、TypeScript、NaiveUI、Vite5、UnoCSS<!--zh-->
5. Direct Postgres connections.<!--en-->
7. **Node** 20 或更高版本<!--zh-->
6. Vue3、pnpm、TypeScript、NaiveUI、Vite5、UnoCSS<!--en-->
8. **pnpm** 9 或更高版本<!--zh-->
7. **Node** 20 or later<!--en-->
<!--zh-->
8. **pnpm** 9 or later<!--en-->
## 限制<!--zh-->

## Limitations<!--en-->
- 日期/时间戳仅比较到秒（格式：DDMMYYYYHH24MISS）。<!--zh-->
<!--en-->
- 不支持的数据类型：blob、long、longraw、bytea。<!--zh-->
- Date/Timestamps compared only to the second (format: DDMMYYYYHH24MISS).<!--en-->
- 布尔类型跨平台比较限制。<!--zh-->
- Unsupported data types: blob, long, longraw, bytea.<!--en-->
- 预留字不能用作表/列名。<!--zh-->
- Cross-platform comparison limitations with boolean type.<!--en-->
- 如果RDBMS原生大小写中的列被引号括起来，则需要在`dc_table_column_map`表中为该列覆盖`preserve_case`。例如，如果Oracle中使用大写引号创建了列（"MYCOL"）。<!--zh-->
- Reserved words cannot be used for table/column names.<!--en-->
<!--zh-->
- If a column is quoted in the RDBMS's native case, you will need to override the `preserve_case` in the `dc_table_column_map` table for that column. For example, if a column was created in Oracle with quotes in upper case ("MYCOL").<!--en-->
# 快速开始<!--zh-->

# Getting Started<!--en-->
## 1. Fork并Star仓库<!--zh-->

## 1. Fork && star the repository<!--en-->
## 2. 克隆和构建<!--zh-->

## 2. Clone and Build<!--en-->
```shell<!--zh-->
<!--en-->
git clone git@github.com:WJX20/dataCompare.git<!--zh-->
```shell<!--en-->
cd dataCompare<!--zh-->
git clone git@github.com:WJX20/dataCompare.git<!--en-->
mvn clean install -U<!--zh-->
cd dataCompare<!--en-->
<!--zh-->
mvn clean install -U<!--en-->
修改 application.yml（redis:你的密码、postgresql:你的密码）<!--zh-->

change application.yml (redis:your password、postgresql:your password)<!--en-->
```运行SQL<!--zh-->
<!--en-->
运行 doc/datacompare.sql<!--zh-->
```run sql<!--en-->
<!--zh-->
run doc/datacompare.sql<!--en-->
```启动<!--zh-->
<!--en-->
运行 DataCompareApplication<!--zh-->
```start<!--en-->
<!--zh-->
run DataCompareApplication<!--en-->
```启动前端dataCompareUI<!--zh-->
<!--en-->
cd dataCompareUI<!--zh-->
```start frontend dataCompareUI<!--en-->
<!--zh-->
cd dataCompareUI<!--en-->
```安装依赖<!--zh-->

```Install dependencies<!--en-->
pnpm install<!--zh-->

pnpm install<!--en-->
```启动<!--zh-->
<!--en-->
pnpm dev<!--zh-->
```start<!--en-->
<!--zh-->
pnpm dev<!--en-->
```构建<!--zh-->
<!--en-->
pnpm build<!--zh-->
```build<!--en-->
<!--zh-->
pnpm build<!--en-->
```在浏览器中输入网站地址<!--zh-->
<!--en-->
http://localhost:9725/<!--zh-->
```Enter the website address in the browser<!--en-->
<!--zh-->
http://localhost:9725/<!--en-->
```登录和密码<!--zh-->

```login && password<!--en-->
默认登录名：admin<!--zh-->
<!--en-->
默认密码：123456<!--zh-->
default login: admin<!--en-->
<!--zh-->
default password: 123456<!--en-->
```注意<!--zh-->
<!--en-->
cd dataCompareUI/package.json<!--zh-->
```notice<!--en-->
我暂时禁用了simple-git-hooks和lint-staged，以防止单个项目前后端的代码错误。因此，我添加了"_"前缀并删除了.git/hooks/commit-msg和.git/hooks/pre-commit。如果需要此功能，只需删除前缀，它将在提交过程中自动生成为.git/hooks/commit-msg和.git/hooks/pre-commit。<!--zh-->
cd dataCompareUI/package.json<!--en-->
<!--zh-->
I temporarily disabled simple-git-hooks and lint-staged to prevent code errors in the front-end and back-end of a single project. Therefore, I added an "_" prefix and removed .git/hooks/commit-msg and .git/hooks/pre-commit. If you need this feature, simply remove the prefix and it will be automatically generated during the commit process as .git/hooks/commit-msg and .git/hooks/pre-commit.<!--en-->
```<!--zh-->

```<!--en-->
# 参考<!--zh-->
<!--en-->
### 系统<!--zh-->
# Reference<!--en-->
<!--zh-->
### System<!--en-->
#### batch-fetch-size<!--zh-->

#### batch-fetch-size<!--en-->
设置从源数据库或目标数据库检索行的获取大小。<!--zh-->

Sets the fetch size for retrieving rows from the source or target database.<!--en-->
默认值：2000<!--zh-->

Default: 2000<!--en-->
#### batch-commit-size<!--zh-->

#### batch-commit-size<!--en-->
提交大小控制插入到dc_source/dc_target临时表中的数组大小和行数。<!--zh-->

The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.<!--en-->
默认值：2000<!--zh-->

Default: 2000<!--en-->
#### batch-progress-report-size<!--zh-->

#### batch-progress-report-size<!--en-->
定义用于报告进度的行数。<!--zh-->

Defines the number of rows used in mod to report progress.<!--en-->
默认值：1000000<!--zh-->

Default: 1000000<!--en-->
#### column-hash-method<!--zh-->

#### column-hash-method<!--en-->
确定如何执行哈希。有效值为`database`和`hybrid`。当设置为`database`时，列值哈希在源/目标数据库上执行。对于`hybrid`，哈希由pgCompare线程执行。<!--zh-->

Determines how the hash is performed. Valid values are `database` and `hybrid`. When set to `database` the column value hash is performed on the source/target database. For `hybrid` the hash is performed by the pgCompare thread.<!--en-->
默认值：database<!--zh-->

Default: database<!--en-->
#### database-sort<!--zh-->

#### database-sort<!--en-->
确定基于主键对行的排序是否发生在源/目标数据库上。如果设置为true（默认值），则在比较之前对行进行排序。如果设置为false，则排序将在仓库数据库中进行。<!--zh-->

Determines if the sorting of the rows based on primary key occurs on the source/target database. If set to true, the default, the rows will be sorted before being compared. If set to false, the sorting will take place in the repository database.<!--en-->
默认值：true<!--zh-->

Default: true<!--en-->
#### float-scale<!--zh-->

#### float-scale<!--en-->
设置用于转换低精度数字的首选精度。<!--zh-->

Set the preferred scale used to cast low precision numbers.<!--en-->
默认值：3<!--zh-->

Default: 3<!--en-->
#### loader-threads<!--zh-->

#### loader-threads<!--en-->
设置加载数据到临时表的线程数。设为0可禁用加载器线程。<!--zh-->

Sets the number of threads to load data into the temporary tables. Set to 0 to disable loader threads.<!--en-->
默认值：0<!--zh-->

Default: 0<!--en-->
#### log-level<!--zh-->

#### log-level<!--en-->
确定写入日志目的地的日志消息数量的级别。<!--zh-->

Level to determine the amount of log messages written to the log destination.<!--en-->
默认值：INFO<!--zh-->

Default: INFO<!--en-->
#### log-destination<!--zh-->

#### log-destination<!--en-->
日志消息将写入的位置。<!--zh-->

Location where log messages will be written.<!--en-->
默认值：stdout<!--zh-->

Default: stdout<!--en-->
#### message-queue-size<!--zh-->

#### message-queue-size<!--en-->
加载器线程使用的消息队列大小（消息数）。<!--zh-->

Size of message queue used by loader threads (nbr messages).<!--en-->
默认值：100<!--zh-->

Default: 100<!--en-->
#### number-cast<!--zh-->

#### number-cast<!--en-->
定义数字如何转换为哈希函数（notation|standard）。有效值是`notation`表示科学记数法，`standard`表示标准数字转换。<!--zh-->

Defines how numbers are cast for hash function (notation|standard). Valid values are `notation` for scientific notation and `standard` for standard number casting.<!--en-->
默认值：notation<!--zh-->

Default: notation<!--en-->
#### observer-throttle<!--zh-->

#### observer-throttle<!--en-->
设置为true或false，指示加载器线程在继续加载更多数据到临时表之前暂停并等待观察者线程跟上。<!--zh-->

Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.<!--en-->
默认值：true<!--zh-->

Default: true<!--en-->
#### observer-throttle-size<!--zh-->

#### observer-throttle-size<!--en-->
加载器线程在休眠并等待观察者线程清除之前加载的行数。<!--zh-->

Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.<!--en-->
默认值：2000000<!--zh-->

Default: 2000000<!--en-->
#### observer-vacuum<!--zh-->

#### observer-vacuum<!--en-->
设置为true或false，指示观察者是否在检查点期间对临时表执行vacuum操作。<!--zh-->

Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.<!--en-->
默认值：true<!--zh-->

Default: true<!--en-->
#### stage-table-parallel<!--zh-->

#### stage-table-parallel<!--en-->
临时表的默认并行度。<!--zh-->

Default parallel degree to set on staging table.<!--en-->
默认值：0<!--zh-->

Default: 0<!--en-->
#### standard-number-format<!--zh-->

#### standard-number-format<!--en-->
用于转换数字的格式<!--zh-->

Format used to cast numbers<!--en-->
默认值：0000000000000000000000.0000000000000000000000<!--zh-->

Default: 0000000000000000000000.0000000000000000000000<!--en-->
#### batch-offset-size<!--zh-->

#### batch-offset-size<!--en-->
此配置表示将跳过前n个数据条目，并从第(n+1)个数据条目开始生成哈希值进行比较。<!--zh-->

This configuration indicates that the first n data entries will be skipped, and the hash values will be generated starting from the (n + 1)th data entry for comparison.<!--en-->
默认值：0<!--zh-->

Default: 0<!--en-->
#### batch-compare-size<!--zh-->

#### batch-compare-size<!--en-->
此配置表示将生成多少个哈希值。<!--zh-->

This configuration indicates how many Hash values will be generated.<!--en-->
默认值：2000<!--zh-->

Default: 2000<!--en-->
"batch-offset-size" & "batch-compare-size"：这两个配置用于在生成"哈希比较"时对数据进行分页查询。例如，仅比较1001到2000或5001到10000范围内的数据。<!--zh-->

"batch-offset-size" & "batch-compare-size": These two configurations are used to paginate the data for querying when generating "hash comparison". For instance, only compare the data ranging from 1001 to 2000 or from 5001 to 10000.<!--en-->
#### batch-check-size<!--zh-->

#### batch-check-size<!--en-->
此配置表示将执行多少次"检查验证"。<!--zh-->

This configuration indicates how many "check validations" are to be performed.<!--en-->
默认值：1000<!--zh-->

Default: 1000<!--en-->
# 图片展示<!--zh-->
<!--en-->
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/3ce94f66-1de6-41c0-8d5c-f7b1bc168775" /><!--zh-->
# Images<!--en-->
<img width="1860" height="923" alt="home" src="https://github.com/user-attachments/assets/6787526d-022c-4d79-93d9-d558c31b545c" /><!--zh-->
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/3ce94f66-1de6-41c0-8d5c-f7b1bc168775" /><!--en-->
<img width="1860" height="923" alt="datasource" src="https://github.com/user-attachments/assets/6d03913c-9579-4097-86ef-e0679de51577" /><!--zh-->
<img width="1860" height="923" alt="home" src="https://github.com/user-attachments/assets/6787526d-022c-4d79-93d9-d558c31b545c" /><!--en-->
<img width="1860" height="923" alt="user" src="https://github.com/user-attachments/assets/96f1c046-52ad-4e0a-a424-53e2b90226b1" /><!--zh-->
<img width="1860" height="923" alt="datasource" src="https://github.com/user-attachments/assets/6d03913c-9579-4097-86ef-e0679de51577" /><!--en-->
<img width="1860" height="923" alt="verify" src="https://github.com/user-attachments/assets/9054f2ac-5ee3-4764-95d1-bcce67e504ca" /><!--zh-->
<img width="1860" height="923" alt="user" src="https://github.com/user-attachments/assets/96f1c046-52ad-4e0a-a424-53e2b90226b1" /><!--en-->
<img width="1860" height="923" alt="verifyConfig" src="https://github.com/user-attachments/assets/ff1b4a39-289f-46d5-af49-75dd35408ffa" /><!--zh-->
<img width="1860" height="923" alt="verify" src="https://github.com/user-attachments/assets/9054f2ac-5ee3-4764-95d1-bcce67e504ca" /><!--en-->
<img width="1562" height="843" alt="verifyDetails" src="https://github.com/user-attachments/assets/de3694a2-4ef6-4ae7-98ae-e1da1d9b9c6b" /><!--zh-->
<img width="1860" height="923" alt="verifyConfig" src="https://github.com/user-attachments/assets/ff1b4a39-289f-46d5-af49-75dd35408ffa" /><!--en-->
<img width="1446" height="836" alt="verifyDiffDetails" src="https://github.com/user-attachments/assets/5e5b6889-d1f7-4559-a730-1479fa093f38" /><!--zh-->
<img width="1562" height="843" alt="verifyDetails" src="https://github.com/user-attachments/assets/de3694a2-4ef6-4ae7-98ae-e1da1d9b9c6b" /><!--en-->
<!--zh-->
<img width="1446" height="836" alt="verifyDiffDetails" src="https://github.com/user-attachments/assets/5e5b6889-d1f7-4559-a730-1479fa093f38" /><!--en-->
# 许可证<!--zh-->

# License<!--en-->
**dataCompare** 根据 [Apache 2.0许可证](LICENSE.md) 授权。<!--zh-->
<!--en-->
**dataCompare** is licensed under the [Apache 2.0 license](LICENSE.md).<!--en-->
