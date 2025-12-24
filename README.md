<div>
  <h1 style="font-size: 70px;text-align: center">data compare</h1>
  <h2 style="text-align: center">Data Compare</h2>
  <h2 style="text-align: center">存储库基于postgresql</h2>
</div>
<hr>

[![License](https://img.shields.io/github/license/CrunchyData/postgres-operator)](LICENSE.md)

# Data Compare Made Simple

**data Compare** is a Java-based tool for validating data consistency after replication or migration between databases. It's designed for scenarios like:


- **Data migration from Oracle/DB2/MariaDB/MySQL/MSSQL to Postgres:**  Compare data post-migration.

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

1. **Java** 17 or later.
2. **Maven** 3.8 or later.
3. **Postgres** 15 or later (for the repository).
4. Supported JDBC drivers (DB2, Postgres, MySQL, MSSQL and Oracle currently supported).
5. Direct Postgres connections.
6. Vue3、pnpm、TypeScript、NaiveUI、Vite5、UnoCSS
7. **node** 20 or later
8. **pnpm** 9 or later

## Limitations

- Date/Timestamps compared only to the second (format: DDMMYYYYHH24MISS).
- Unsupported data types: blob, long, longraw, bytea.
- Cross-platform comparison limitations with boolean type.
- Reserved words cannot be used for table/column names.
- If a column is quoted in the RDBMS's native case, you will need to override the `preserve_case` in the `dc_table_column_map` table for that column.  For example, if a column was created in Oracle with quotes in upper case ("MYCOL").

# Getting Started

## 1. Fork && star the repository

## 2. Clone and Build

```shell
git clone git@github.com:WJX20/dataCompare.git
cd dataCompare
mvn clean install -U

change application.yml(redis:your password、postgresql:your password)

```run sql
run doc/datacompare.sql

```start
run DataCompareApplication


```start frontenddataCompareUI
cd dataCompareUI

```Install dependencies

pnpm install

```start
pnpm dev

```build
pnpm build
```


# Reference
### System

### System

#### batch-fetch-size

  Sets the fetch size for retrieving rows from the source or target database.

  Default:  2000

#### batch-commit-size

  The commit size controls the array size and number of rows concurrently inserted into the dc_source/dc_target staging tables.

  Default: 2000

#### batch-progress-report-size

  Defines the number of rows used in mod to report progress.

  Default: 1000000

#### column-hash-method

  Determines how the hash is performed.  Valid values are `database` and `hybrid`.  When set to `database` the column value hash is performed on the source/target database.  For `hybrid` the hash is performed by the pgCompare thread.

  Default:  database

#### database-sort

  Determines if the sorting of the rows based on primary key occurs on the source/target database.  If set to true, the default, the rows will be sorted before being compared.  If set to false, the sorting will take place in the repository database.

  Default: true

#### float-scale

  Set the preferred scale used to cast low precision numbers.

  Default: 3

#### loader-threads

  Sets the number of threads to load data into the temporary tables. Set to 0 to disable loader threads.

  Default: 0

#### log-level
  
  Level to determine the amount of log messages written to the log destination.

  Default: INFO

#### log-destination

  Location where log messages will be written.

  Default:  stdout

#### message-queue-size

  Size of message queue used by loader threads (nbr messages).
  
  Default: 100

#### number-cast

  Defines how numbers are cast for hash function (notation|standard).  Valid values are `notation` for scientific notation and `standard` for standard number casting.
  
  Default: notation

#### observer-throttle

  Set to true or false, instructs the loader threads to pause and wait for the observer thread to catch up before continuing to load more data into the staging tables.

  Default: true

#### observer-throttle-size

  Number of rows loaded before the loader thread will sleep and wait for clearance from the observer thread.

  Default: 2000000

#### observer-vacuum

  Set to true or false, instructs the observer whether to perform a vacuum on the staging tables during checkpoints.

  Default: true

#### stage-table-parallel

  Default parallel degree to set on staging table.

  Default: 0

#### standard-number-format
  
  Format used to cast numbers 
  
  Default: 0000000000000000000000.0000000000000000000000

#### batch-offset-size
  
  This configuration indicates that the first n data entries will be skipped, and the hash values will be generated starting from the (n + 1)th data entry for comparison. 
  
  Default: 0
  
#### batch-compare-size
  
  This configuration indicates how many Hash values will be generated. 
  
  Default: 2000
  
"batch-offset-size" & "batch-compare-size": These two configurations are used to paginate the data for querying when generating "hash comparison". For instance, only compare the data ranging from 1001 to 2000 or from 5001 to 10000.

#### batch-check-size
  
  This configuration indicates how many "check validations" are to be performed. 
  
  Default: 1000

# Img
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/3ce94f66-1de6-41c0-8d5c-f7b1bc168775" />
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/f777ce63-fd6b-41ae-8257-b983e9e043a6" />
<img width="1860" height="923" alt="datasource" src="https://github.com/user-attachments/assets/6d03913c-9579-4097-86ef-e0679de51577" />
<img width="1860" height="923" alt="datasource" src="https://github.com/user-attachments/assets/573e3ca2-0656-4c32-af1b-c9fb4a711d78" />
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/f84ddbc8-3d87-4e40-9865-df40302740f4" />
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/95f4a0ab-d8c9-4602-883b-af551e369d0e" />
<img width="1860" height="923" alt="login" src="https://github.com/user-attachments/assets/d3a7759b-9171-4dbe-9950-5bf606486232" />


# License

**dataCompare** is licensed under the [Apache 2.0 license](LICENSE.md).



