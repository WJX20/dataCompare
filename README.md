<div>
  <h1 style="font-size: 70px;text-align: center">data compare</h1>
  <h2 style="text-align: center">Data Compare</h2>
  <h2 style="text-align: center">存储库基于postgresql</h2>
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

**pgCompare** is a Java-based tool for validating data consistency after replication or migration between databases. It's designed for scenarios like:


- **Data migration from Oracle/DB2/MariaDB/MySQL/MSSQL to Postgres:**  Compare data during or post-migration.

- **Logical replication between same or different database platforms:** Validate data across platforms while minimizing database overhead.

- **Active-Active replication configuration:**  Regularly verify data consistency to mitigate risks.

DataCompare uses hashing to compare table data efficiently. Hash values for primary keys and remaining columns are stored in a repository, reducing storage and network demands. Comparisons are processed in parallel, improving performance.


# Features

- Supports Oracle, PostgreSQL, DB2, MariaDB, MySQL, and MSSQL.
- Efficient parallel comparisons using hashing.
- Handles batch processing for performance tuning.
- Stores configurations for multiple comparison projects in a central repository.

# Installation

## Requirements

Before initiating the build and installation process, ensure the following prerequisites are met:

1. **Java** 21 or later.
2. **Maven** 3.9 or later.
3. **Postgres** 15 or later (for the repository).
4. Supported JDBC drivers (DB2, Postgres, MySQL, MSSQL and Oracle currently supported).
5. Direct Postgres connections (e.g., no pgBouncer).

## Limitations

- Date/Timestamps compared only to the second (format: DDMMYYYYHH24MISS).
- Unsupported data types: blob, long, longraw, bytea.
- Cross-platform comparison limitations with boolean type.
- Reserved words cannot be used for table/column names.
- If a column is quoted in the RDBMS's native case, you will need to override the `preserve_case` in the `dc_table_column_map` table for that column.  For example, if a column was created in Oracle with quotes in upper case ("MYCOL").

# Getting Started

## 1. Fork the repository

## 2. Clone and Build

```shell
git clone --depth 1 git@github.com:<your-github-username>/pgCompare.git
cd DataCompare
mvn clean install
```

# Viewing Results

## Summary from Last Run

```sql
WITH mr AS (SELECT max(rid) rid FROM dc_result)
SELECT compare_start, table_name, status, equal_cnt+not_equal_cnt+missing_source_cnt+missing_target_cnt  AS total_cnt,
       equal_cnt, not_equal_cnt, missing_source_cnt + missing_target_cnt AS missing_cnt
FROM dc_result r
         JOIN mr ON (mr.rid = r.rid)
ORDER BY table_name;
```

## Out-of-Sync Rows

```sql
SELECT COALESCE(s.table_name, t.table_name) AS table_name,
       CASE
           WHEN s.compare_result = 'n' THEN 'out-of-sync'
           WHEN s.compare_result = 'm' THEN 'missing target'
           WHEN t.compare_result = 'm' THEN 'missing source'
           END AS compare_result,
       COALESCE(s.pk, t.pk) AS primary_key
FROM dc_source s
         FULL OUTER JOIN dc_target t ON s.pk = t.pk and s.tid=t.tid;
```

# Reference
### System
- batch-fetch-size: Sets the fetch size for retrieving rows from the source or target database.
- batch-commit-size:  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.
- batch-progress-report-size:  Defines the number of rows used in mod to report progress.
- database-source:  Determines if the sorting of the rows based on primary key occurs on the source/target database.  If set to true, the default, the rows will be sorted before being compared.  If set to false, the sorting will take place in the repository database.
- loader-threads: Sets the number of threads to load data into the temporary tables. Default is 4.  Set to 0 to disable loader threads.
- log-level:   Level to determine the amount of log messages written to the log destination.
- log-destination:  Location where log messages will be written.  Default is stdout.
- message-queue-size:  Size of message queue used by loader threads (nbr messages).  Default is 100.
- number-cast: Defines how numbers are cast for hash function (notation|standard).  Default is notation (for scientific notation).
- observer-throttle:  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.
- observer-throttle-size:  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.
- observer-vacuum:  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.

### Repository
- repo-dbname:  Repository database name.
- repo-host: Host name of server hosting the Postgres repository database.
- repo-password:  Postgres database user password.
- repo-port:  Repository Postgres instance port.
- repo-schema:  Name of schema that owns the repository tables.
- repo-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- repo-user:  Postgres database username.

### Source

- source-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).
- source-dbname:  Database or service name.
- source-host:  Database server name.
- source-password:  Database password.
- source-port:  Database port.
- source-schema:  Name of schema that owns the tables.
- source-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- source-type:  Database type: oracle, postgres
- source-user:   Database username.

### Target

- target-database-hash: True or false, instructs the application where the hash should be computed (on the database or by the application).
- target-dbname:  Database or service name.
- target-host:  Database server name.
- target-password:  Database password.
- target-port:  Database port.
- target-schema:  Name of schema that owns the tables.
- target-sslmode: Set the SSL mode to use for the database connection (disable|prefer|require)
- target-type:  Database type: oracle, postgres
- target-user:  Database username.

# License

**dataCompare** is licensed under the [Apache 2.0 license](LICENSE.md).



**springboot版本**

## springboot版本的数据对比后端，支持Oracle,mysql,sqlserver,pg等等

## **mvn clean install -U**

## **运行pgApplication**
