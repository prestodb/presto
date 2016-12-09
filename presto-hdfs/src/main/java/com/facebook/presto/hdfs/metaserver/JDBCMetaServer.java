/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hdfs.metaserver;

import com.facebook.presto.hdfs.HDFSColumnHandle;
import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.hdfs.HDFSDatabase;
import com.facebook.presto.hdfs.HDFSTable;
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.hdfs.HDFSTableLayoutHandle;
import com.facebook.presto.hdfs.exception.ArrayLengthNotMatchException;
import com.facebook.presto.hdfs.exception.RecordMoreLessException;
import com.facebook.presto.hdfs.exception.TypeUnknownException;
import com.facebook.presto.hdfs.jdbc.JDBCDriver;
import com.facebook.presto.hdfs.jdbc.JDBCRecord;
import com.facebook.presto.hdfs.type.UnknownType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class JDBCMetaServer
implements MetaServer
{
    private static final Logger log = Logger.get(JDBCMetaServer.class);
    private static final Map<String, String> sqlTable = new HashMap<>();

    private final JDBCDriver jdbcDriver;
    private final HDFSConfig config;

    // read config. check if meta table already exists in database, or else initialize missing tables.
    public JDBCMetaServer(HDFSConfig config)
    {
        // initialize a jdbc driver
        this.config = requireNonNull(config);
        jdbcDriver = new JDBCDriver(
                this.config.getJdbcDriver(),
                this.config.getMetaserverUser(),
                this.config.getMetaserverPass(),
                this.config.getMetaserverUri());

        sqlTable.putIfAbsent("dbs",
                "CREATE TABLE DBS(DB_ID BIGSERIAL PRIMARY KEY, DB_DESC varchar(4000), DB_NAME varchar(128) UNIQUE, DB_LOCATION_URI varchar(4000), DB_OWNER varchar(128));");
        // TBL_NAME: db_name.tbl_name
        sqlTable.putIfAbsent("tbls",
                "CREATE TABLE TBLS(TBL_ID BIGSERIAL PRIMARY KEY, DB_NAME varchar(128), TBL_NAME varchar(256) UNIQUE, TBL_LOCATION_URI varchar(4000));");
        sqlTable.putIfAbsent("tbl_params",
                "CREATE TABLE TBL_PARAMS(TBL_PARAM_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(128), FIBER_COL bigint, TIME_COL bigint, FIBER_FUNC varchar(4000));");
        // COL_NAME: TBL_NAME.col_name;  COL_TYPE: FIBER_COL|TIME_COL|REGULAR;   TYPE:INT|DECIMAL|STRING|... refer to spi.StandardTypes
        sqlTable.putIfAbsent("cols",
                "CREATE TABLE COLS(COL_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(256), COL_NAME varchar(384) UNIQUE, COL_TYPE varchar(10), TYPE varchar(20));");
        sqlTable.putIfAbsent("fibers",
                "CREATE TABLE FIBERS(INDEX_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(256), FIBER bigint, TIME_BEGIN timestamp, TIME_END timestamp);");

        // initalise meta tables
        checkJDBCMetaExists();
    }

    // check if meta tables already exist, if not, create them.
    private void checkJDBCMetaExists()
    {
        DatabaseMetaData dbmeta = jdbcDriver.getDbMetaData();
        if (dbmeta == null) {
            log.error("database meta is null");
        }
        for (String tbl : sqlTable.keySet()) {
            try (ResultSet rs = dbmeta.getTables(null, null, tbl, null)) {
                if (!rs.next()) {
                    if (jdbcDriver.executeUpdate(sqlTable.get(tbl)) != 0) {
                        log.error("DDL execution failed.");
                    }
                }
            } catch (SQLException e) {
                log.error(e, "jdbc meta getTables error");
            }
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        List<JDBCRecord> records;
        List<String> resultL = new ArrayList<>();
        String sql = "SELECT db_name FROM dbs;";
        String[] fields = {"db_name"};
        records = jdbcDriver.executreQuery(sql, fields);
        for (JDBCRecord record : records) {
            resultL.add(record.getString(fields[0]));
        }
        return resultL;
    }

    private Optional<HDFSDatabase> getDatabase(String databaseName)
    {
        List<JDBCRecord> records;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT db_name, db_desc, db_location_uri, db_owner FROM dbs WHERE db_name='")
                .append(databaseName)
                .append("';");
        String[] fields = {"db_name", "db_desc", "db_location_uri", "db_owner"};
        records = jdbcDriver.executreQuery(sb.toString(), fields);
        if (records.size() != 1) {
            log.error("getDatabase JDBC query error! More/Less than one database matches");
            return Optional.empty();
        }
        JDBCRecord record = records.get(0);
        try {
            HDFSDatabase database = new HDFSDatabase(
                    requireNonNull(record.getString("db_name")),
                    requireNonNull(record.getString("db_desc")),
                    requireNonNull(record.getString("db_location_uri")),
                    requireNonNull(record.getString("db_owner")));
            return Optional.of(database);
        }
        catch (NullPointerException e) {
            return Optional.empty();
        }
    }

    private Optional<List<String>> getAllTables(String databaseName)
    {
        List<JDBCRecord> records;
        List<String> resultL = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT tbl_name FROM tbls WHERE db_name='")
                .append(databaseName)
                .append("';");
        String[] fields = {"tbl_name"};
        records = jdbcDriver.executreQuery(sql.toString(), fields);
        if (records.size() == 0) {
            return Optional.empty();
        }
        for (JDBCRecord record : records) {
            resultL.add(record.getString(fields[0].split(".")[1]));
        }
        return Optional.of(resultL);
    }

    @Override
    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        List<JDBCRecord> records;
        List<SchemaTableName> tables = new ArrayList<>();
        String tableName;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT tbl_name FROM tbls WHERE tbl_name='")
                .append(prefix)
                .append("';");
        String[] fields = {"tbl_name"};
        records = jdbcDriver.executreQuery(sql.toString(), fields);
        if (records.size() == 0) {
            return tables;
        }
        for (JDBCRecord record : records) {
            tableName = record.getString(fields[0]);
            tables.add(new SchemaTableName(tableName.split(".")[0], tableName.split(".")[1]));
        }
        return tables;
    }

    private Optional<HDFSTable> getTable(String databaseName, String tableName)
    {
        HDFSTableHandle table;
        HDFSTableLayoutHandle tableLayout;
        List<HDFSColumnHandle> columns;
        List<ColumnMetadata> metadatas;

        Optional<HDFSTableHandle> tableOptional = getTableHandle(databaseName, tableName);
        if (!tableOptional.isPresent()) {
            log.warn("Table not exists");
            return Optional.empty();
        }
        table = tableOptional.get();

        Optional<HDFSTableLayoutHandle> tableLayoutOptional = getTableLayout(databaseName, tableName);
        if (!tableLayoutOptional.isPresent()) {
            log.warn("Table layout not exists");
            return Optional.empty();
        }
        tableLayout = tableLayoutOptional.get();

        Optional<List<HDFSColumnHandle>> columnsOptional = getTableColumnHandle(databaseName, tableName);
        if (!columnsOptional.isPresent()) {
            log.warn("Column handles not exists");
            return Optional.empty();
        }
        columns = columnsOptional.get();

        Optional<List<ColumnMetadata>> metadatasOptional = getTableColMetadata(databaseName, tableName);
        if (!metadatasOptional.isPresent()) {
            log.warn("Column metadata not exists");
            return Optional.empty();
        }
        metadatas = metadatasOptional.get();

        HDFSTable hdfsTable = new HDFSTable(table, tableLayout, columns, metadatas);
        return Optional.of(hdfsTable);
    }

    @Override
    public Optional<HDFSTableHandle> getTableHandle(String databaseName, String tableName)
    {
        HDFSTableHandle table;
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT tbl_name, tbl_location_uri FROM tbls WHERE tbl_name='")
                .append(databaseName)
                .append(".")
                .append(tableName)
                .append("';");
        String[] fields = {"tbl_name", "tbl_location_uri"};
        records = jdbcDriver.executreQuery(sql.toString(), fields);
        if (records.size() != 1) {
            log.error("Match more/less than one table");
            return Optional.empty();
        }
        JDBCRecord record = records.get(0);
        String schema = record.getString(fields[0]);
        String location = record.getString(fields[1]);
        table = new HDFSTableHandle(requireNonNull(schema.split(".")[0], "database name is null"),
                requireNonNull(schema.split(".")[1], "table name is null"),
                requireNonNull(location, "location uri is null"));
        return Optional.of(table);
    }

    @Override
    public Optional<HDFSTableLayoutHandle> getTableLayout(String databaseName, String tableName)
    {
        HDFSTableLayoutHandle tableLayout;
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT fiber_col, time_col, fiber_func FROM tbl_params WHERE tbl_name='")
                .append(databaseName)
                .append(".")
                .append(tableName)
                .append("';");
        String[] fields = {"fiber_col", "time_col", "fiber_func"};
        records = jdbcDriver.executreQuery(sql.toString(), fields);
        if (records.size() != 1) {
            log.error("Match more/less than one table");
            return Optional.empty();
        }
        JDBCRecord record = records.get(0);
        String fiberColName = record.getString(fields[0]);
        String timeColName = record.getString(fields[1]);
        String fiberFunc = record.getString(fields[2]);
        records.clear();

        // construct ColumnHandle
        HDFSColumnHandle fiberCol = getColumnHandle(fiberColName);
        HDFSColumnHandle timeCol = getColumnHandle(timeColName);

        tableLayout = new HDFSTableLayoutHandle(tableName, fiberCol, timeCol, fiberFunc);
        return Optional.of(tableLayout);
    }

    /**
     * Get all column handles of specified table
     * */
    public Optional<List<HDFSColumnHandle>> getTableColumnHandle(String databaseName, String tableName)
    {
        List<HDFSColumnHandle> columnHandles = new ArrayList<>();
        List<JDBCRecord> records;
        String colName;
        String databaseTableName = formName(databaseName, tableName);
        String sql = "SELECT col_name FROM cols WHERE tbl_name='"
                + databaseTableName
                + "';";
        String[] colFields = {"tbl_name"};
        records = jdbcDriver.executreQuery(sql, colFields);
        if (records.size() == 0) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        for (JDBCRecord record : records) {
            colName = formName(databaseName, tableName, record.getString(colFields[0]));
            columnHandles.add(getColumnHandle(colName));
        }
        return Optional.of(columnHandles);
    }

    private HDFSColumnHandle getColumnHandle(String databaseTableColName)
    {
        String databaseName = getDatabaseName(databaseTableColName);
        String tableName = getTableName(databaseTableColName);
        String colName = getColName(databaseTableColName);
        StringBuilder name = new StringBuilder();
        name.append(requireNonNull(databaseName, "databaseName is null"))
                .append(".")
                .append(requireNonNull(tableName, "tableName is null"))
                .append(".")
                .append(requireNonNull(colName, "colName is null"));
        return getColumnHandle(colName, name.toString());
    }

    private HDFSColumnHandle getColumnHandle(String colName, String databaseTableColName)
    {
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT col_type, type FROM cols WHERE col_name='")
                .append(databaseTableColName)
                .append("';");
        String[] colFields = {"col_type", "type"};
        records = jdbcDriver.executreQuery(sql.toString(), colFields);
        if (records.size() != 1) {
            log.error("Match more/less than one table");
            throw new RecordMoreLessException();
        }
        JDBCRecord fiberColRecord = records.get(0);
        String colTypeName = fiberColRecord.getString(colFields[0]);
        String typeName = fiberColRecord.getString(colFields[1]);
        records.clear();
        // Deal with colType
        HDFSColumnHandle.ColumnType colType = getColType(colTypeName);
        if (colType == HDFSColumnHandle.ColumnType.NOTVALID) {
            log.error("Col type not match!");
            throw new RecordMoreLessException();
        }
        // Deal with type
        Type type = getType(typeName);
        if (type == UnknownType.UNKNOWN) {
            log.error("Type unknown!");
            throw new TypeUnknownException();
        }
        return new HDFSColumnHandle(colName, type, "", colType);
    }

    public Optional<List<ColumnMetadata>> getTableColMetadata(String databaseName, String tableName)
    {
        List<ColumnMetadata> colMetadatas = new ArrayList<>();
        List<JDBCRecord> records;
        String colName;
        String tblName = formName(databaseName, tableName);
        String sql = "SELECT col_name FROM cols WHERE tbl_name='"
                + tblName
                + "';";
        String[] colFields = {"col_name"};
        records = jdbcDriver.executreQuery(sql, colFields);
        if (records.size() == 0) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        for (JDBCRecord record : records) {
            colName = formName(databaseName, tableName, record.getString(colFields[0]));
            colMetadatas.add(getColMetadata(colName));
        }
        return Optional.of(colMetadatas);
    }

    private ColumnMetadata getColMetadata(String databaseTableColName)
    {
        String colName = getColName(databaseTableColName);
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT type FROM cols WHERE col_name='")
                .append(databaseTableColName)
                .append("';");
        String[] colFields = {"type"};
        records = jdbcDriver.executreQuery(sql.toString(), colFields);
        if (records.size() != 1) {
            log.error("Match more/less than one table");
            throw new RecordMoreLessException();
        }
        JDBCRecord colRecord = records.get(0);
        String typeName = colRecord.getString(colFields[0]);
        Type type = getType(typeName);
        if (type == UnknownType.UNKNOWN) {
            log.error("Type unknown!");
            throw new TypeUnknownException();
        }
        return new ColumnMetadata(colName, type, "", false);
    }

    private HDFSColumnHandle.ColumnType getColType(String typeName)
    {
        switch (typeName) {
            case "FIBER_COL": return HDFSColumnHandle.ColumnType.FIBER_COL;
            case "TIME_COL": return HDFSColumnHandle.ColumnType.TIME_COL;
            case "REGULAR": return HDFSColumnHandle.ColumnType.REGULAR;
            default : log.error("No match col type!");
                return HDFSColumnHandle.ColumnType.NOTVALID;
        }
    }

    private Type getType(String typeName)
    {
        // TODO fill all common types
        switch (typeName) {
            case "INT": return IntegerType.INTEGER;
            case "DOUBLE": return DoubleType.DOUBLE;
            case "VARCHAR": return VarcharType.VARCHAR;  // TODO add length info to createVarChar
            default: return UnknownType.UNKNOWN;
        }
    }

    // get database name from database.table[.col]
    private String getDatabaseName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split(".");
        if (names.length < 1) {
            throw new ArrayLengthNotMatchException();
        }
        return names[0];
    }

    // get table name from database.table[.col]
    private String getTableName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split(".");
        if (names.length < 2) {
            throw new ArrayLengthNotMatchException();
        }
        return names[1];
    }

    // get col name from database.table.col
    private String getColName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split(".");
        if (names.length < 3) {
            throw new ArrayLengthNotMatchException();
        }
        return names[2];
    }

    // form concatenated name from database and table
    private String formName(String database, String table)
    {
        return database + "." + table;
    }

    // from concatenated name from database and table and col
    private String formName(String database, String table, String col)
    {
        return database + "." + table + "." + col;
    }

    @Override
    public void createDatabase(ConnectorSession session, HDFSDatabase database)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO dbs(db_name, db_desc, db_location_uri, db_owner) VALUES(")
                .append("'").append(database.getName()).append("', ")
                .append("'").append(database.getComment()).append("', ")
                .append("'").append(database.getLocation()).append("', ")
                .append("'").append(database.getOwner()).append("'")
                .append(");");
        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
            log.error("Create database" + database.getName() + " failed!");
        }
    }

    private boolean isDatabaseEmpty(ConnectorSession session, String databaseName)
    {
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(tbl_name) as cnt FROM tbls WHERE db_name='")
                .append(databaseName)
                .append("';");
        String[] fields = {"cnt"};
        records = jdbcDriver.executreQuery(sql.toString(), fields);
        if (records.size() != 1) {
            log.error("isDatabaseEmpty xecution error!");
        }
        JDBCRecord record = records.get(0);
        int count = record.getInt(fields[0]);
        return count == 0 ? true : false;
    }

//    @Override
//    public void dropDatabase(ConnectorSession session, String databaseName)
//    {
//        StringBuilder sql = new StringBuilder();
//        sql.append("DELETE FROM dbs WHERE db_name='")
//                .append(databaseName)
//                .append("';");
//        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
//            log.warn("Drop database" + databaseName + " failed!");
//        }
//    }

//    @Override
//    public void renameDatabase(ConnectorSession session, String source, String target)
//    {
//        StringBuilder sql = new StringBuilder();
//        sql.append("UPDATE dbs SET db_name='")
//                .append(target)
//                .append("' WHERE db_name='")
//                .append(source)
//                .append("';");
//        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
//            log.warn("Rename database from " + source + " to " + target + " failed!");
//        }
//    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        String tableName = tableMetadata.getTable().getTableName();
        String schemaName = tableMetadata.getTable().getSchemaName();
        List<ColumnMetadata> colums = tableMetadata.getColumns();

        // get some more properties
        String location = (String) tableMetadata.getProperties().get("location");
        HDFSColumnHandle fiberCol = (HDFSColumnHandle) tableMetadata.getProperties().getOrDefault("fiber_col", "");
        HDFSColumnHandle timeCol = (HDFSColumnHandle) tableMetadata.getProperties().getOrDefault("time_col", "");
        String fiberFunc = (String) tableMetadata.getProperties().getOrDefault("fiber_func", "");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO tbls(db_name, tbl_name, tbl_location_uri) VALUES('")
                .append(schemaName)
                .append("', '")
                .append(formName(schemaName, tableName))
                .append("', '")
                .append(location)
                .append("');")
                .append("INSERT INTO tbl_params(tbl_name, fiber_col, time_col, fiber_func) VALUES('")
                .append(formName(schemaName, tableName))
                .append("', '")
                .append(fiberCol)
                .append("', '")
                .append(timeCol)
                .append("', '")
                .append(fiberFunc)
                .append("');");
        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
            log.error("Create table " + formName(schemaName, tableName) + " failed!");
        }

        for (ColumnMetadata col : colums) {
            sql.delete(0, sql.length() - 1);
            sql.append("INSERT INTO cols(tbl_name, col_name, type) VALUES('")
                    .append(formName(schemaName, tableName))
                    .append("', '")
                    .append(formName(schemaName, tableName, col.getName()))
                    .append("', '")
                    .append(col.getType())
                    .append("');");
            if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
                log.error("Create cols for table " + formName(schemaName, tableName) + " failed!");
            }
        }
    }

//    @Override
//    public void dropTable(ConnectorSession session, String databaseName, String tableName)
//    {
//        StringBuilder sql = new StringBuilder();
//        sql.append("DELETE FROM tbls WHERE tbl_name='")
//                .append(new SchemaTableName(databaseName, tableName).toString())
//                .append("';");
//        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
//            log.error("Drop table " + databaseName + "." + tableName + " failed!");
//        }
//    }

//    @Override
//    public void renameTable(ConnectorSession session, String databaseName, String tableName, String newDatabaseName, String newTableName)
//    {
//        StringBuilder sql = new StringBuilder();
//        sql.append("")
//    }
}
