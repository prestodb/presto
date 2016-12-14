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
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.hdfs.HDFSTableLayoutHandle;
import com.facebook.presto.hdfs.exception.RecordMoreLessException;
import com.facebook.presto.hdfs.exception.TypeUnknownException;
import com.facebook.presto.hdfs.fs.FSFactory;
import com.facebook.presto.hdfs.jdbc.JDBCDriver;
import com.facebook.presto.hdfs.jdbc.JDBCRecord;
import com.facebook.presto.hdfs.type.UnknownType;
import com.facebook.presto.hdfs.util.Utils;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private FileSystem fileSystem;

    // read config. check if meta table already exists in database, or else initialize tables.
    @Inject
    public JDBCMetaServer()
    {
        // initialize a jdbc driver
        jdbcDriver = new JDBCDriver(
                HDFSConfig.getJdbcDriver(),
                HDFSConfig.getMetaserverUser(),
                HDFSConfig.getMetaserverPass(),
                HDFSConfig.getMetaserverUri());

        log.debug("Config: " + HDFSConfig.getJdbcDriver()
            + "; "
            + HDFSConfig.getMetaserverUri()
            + "; "
            + HDFSConfig.getMetaserverUser()
            + "; "
            + HDFSConfig.getMetaserverPass()
            + "; "
            + HDFSConfig.getMetaserverStore());

        sqlTable.putIfAbsent("dbs",
                "CREATE TABLE DBS(DB_ID BIGSERIAL PRIMARY KEY, DB_DESC varchar(4000), DB_NAME varchar(128) UNIQUE, DB_LOCATION_URI varchar(4000), DB_OWNER varchar(128));");
        // TBL_NAME: db_name.tbl_name
        sqlTable.putIfAbsent("tbls",
                "CREATE TABLE TBLS(TBL_ID BIGSERIAL PRIMARY KEY, DB_NAME varchar(128), TBL_NAME varchar(256) UNIQUE, TBL_LOCATION_URI varchar(4000));");
        sqlTable.putIfAbsent("tbl_params",
                "CREATE TABLE TBL_PARAMS(TBL_PARAM_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(128), FIBER_COL varchar(384), TIME_COL varchar(384), FIBER_FUNC varchar(4000));");
        // COL_NAME: TBL_NAME.col_name;  COL_TYPE: FIBER_COL|TIME_COL|REGULAR;   TYPE:INT|DECIMAL|STRING|... refer to spi.StandardTypes
        sqlTable.putIfAbsent("cols",
                "CREATE TABLE COLS(COL_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(256), COL_NAME varchar(384) UNIQUE, COL_TYPE varchar(10), TYPE varchar(20));");
        sqlTable.putIfAbsent("fibers",
                "CREATE TABLE FIBERS(INDEX_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(256), FIBER bigint, TIME_BEGIN timestamp, TIME_END timestamp);");

        fileSystem = FSFactory.getFS(HDFSConfig.getMetaserverStore()).get();

        // initialise meta tables
        initMeta();
    }

    // check if meta tables already exist, if not, create them.
    private void initMeta()
    {
        log.debug("Init meta data in jdbc meta store");
        int initFlag = 0;
        DatabaseMetaData dbmeta = jdbcDriver.getDbMetaData();
        if (dbmeta == null) {
            log.error("database meta is null");
        }
        for (String tbl : sqlTable.keySet()) {
            assert dbmeta != null;
            try (ResultSet rs = dbmeta.getTables(null, null, tbl, null)) {
                // if table exists
                if (rs.next()) {
                    initFlag++;
                    log.info("Table " + tbl + " already exists.");
                }
            } catch (SQLException e) {
                log.error(e, "jdbc meta getTables error");
            }
        }
        // if no table exists, init all
        if (initFlag == 0) {
            sqlTable.keySet().forEach(
                    tbl -> {
                        if (jdbcDriver.executeUpdate(sqlTable.get(tbl)) == 0) {
                            log.info("Create table" + tbl + " in metaserver");
                        }
                        else {
                            log.error(tbl + " table creation in metaserver execution failed.");
                        }
                    }
            );
            HDFSDatabase defaultDB = new HDFSDatabase("default");
            createDatabase(defaultDB);
        }
        // if not all tables exist, throw an error and break
        else if (initFlag != 5) {
            log.error("Tables not complete!");
            // TODO do not break brutally
            System.exit(1);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        log.debug("Get all databases");
        List<JDBCRecord> records;
        List<String> resultL = new ArrayList<>();
        String sql = "SELECT db_name FROM dbs;";
        String[] fields = {"db_name"};
        records = jdbcDriver.executreQuery(sql, fields);
        records.forEach(record -> resultL.add(record.getString(fields[0])));
        return resultL;
    }

//    private Optional<HDFSDatabase> getDatabase(String databaseName)
//    {
//        log.debug("Get database " + databaseName);
//        List<JDBCRecord> records;
//        String sql = "SELECT db_name, db_desc, db_location_uri, db_owner FROM dbs WHERE db_name='"
//                + databaseName
//                + "';";
//        String[] fields = {"db_name", "db_desc", "db_location_uri", "db_owner"};
//        records = jdbcDriver.executreQuery(sql, fields);
//        if (records.size() != 1) {
//            log.error("getDatabase JDBC query error! More/Less than one database matches");
//            return Optional.empty();
//        }
//        JDBCRecord record = records.get(0);
//        try {
//            HDFSDatabase database = new HDFSDatabase(
//                    requireNonNull(record.getString("db_name")));
//            return Optional.of(database);
//        }
//        catch (NullPointerException e) {
//            return Optional.empty();
//        }
//    }

//    private Optional<List<String>> getAllTables(String databaseName)
//    {
//        log.debug("Get all tables in database " + databaseName);
//        List<JDBCRecord> records;
//        List<String> resultL = new ArrayList<>();
//        String sql = "SELECT tbl_name FROM tbls WHERE db_name='"
//                + databaseName
//                + "';";
//        String[] fields = {"tbl_name"};
//        records = jdbcDriver.executreQuery(sql, fields);
//        if (records.size() == 0) {
//            return Optional.empty();
//        }
//        records.forEach(record -> resultL.add(record.getString(fields[0].split(".")[1])));
//        return Optional.of(resultL);
//    }

    @Override
    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        log.debug("List all tables with prefix " + prefix.toString());
        List<JDBCRecord> records;
        List<SchemaTableName> tables = new ArrayList<>();
        String dbPrefix = prefix.getSchemaName();
        log.debug("listTables dbPrefix: " + dbPrefix);
        String tblPrefix = prefix.getTableName();
        log.debug("listTables tblPrefix: " + tblPrefix);
        StringBuilder baseSql = new StringBuilder("SELECT tbl_name FROM tbls");
        // if dbPrefix not mean to match all
        if (dbPrefix != null) {
            baseSql.append(" WHERE db_name='").append(dbPrefix).append("'");
            // if tblPrefix not mean to match all
            if (tblPrefix != null) {
                baseSql.append(" AND tbl_name='").append(Utils.formName(dbPrefix, tblPrefix)).append("'");
            }
        }
        baseSql.append(";");
        String tableName;
        String[] fields = {"tbl_name"};
        records = jdbcDriver.executreQuery(baseSql.toString(), fields);
        if (records.size() == 0) {
            return tables;
        }
        for (JDBCRecord record : records) {
            tableName = record.getString(fields[0]);
            log.debug("listTables tableName: " + tableName);
            tables.add(new SchemaTableName(Utils.getDatabaseName(tableName), Utils.getTableName(tableName)));
        }
        return tables;
    }

//    private Optional<HDFSTable> getTable(String databaseName, String tableName)
//    {
//        log.debug("Get table " + formName(databaseName, tableName));
//        HDFSTableHandle table;
//        HDFSTableLayoutHandle tableLayout;
//        List<HDFSColumnHandle> columns;
//        List<ColumnMetadata> metadatas;
//
//        Optional<HDFSTableHandle> tableOptional = getTableHandle(databaseName, tableName);
//        if (!tableOptional.isPresent()) {
//            log.warn("Table not exists");
//            return Optional.empty();
//        }
//        table = tableOptional.get();
//
//        Optional<HDFSTableLayoutHandle> tableLayoutOptional = getTableLayout(databaseName, tableName);
//        if (!tableLayoutOptional.isPresent()) {
//            log.warn("Table layout not exists");
//            return Optional.empty();
//        }
//        tableLayout = tableLayoutOptional.get();
//
//        Optional<List<HDFSColumnHandle>> columnsOptional = getTableColumnHandle(databaseName, tableName);
//        if (!columnsOptional.isPresent()) {
//            log.warn("Column handles not exists");
//            return Optional.empty();
//        }
//        columns = columnsOptional.get();
//
//        Optional<List<ColumnMetadata>> metadatasOptional = getTableColMetadata(databaseName, tableName);
//        if (!metadatasOptional.isPresent()) {
//            log.warn("Column metadata not exists");
//            return Optional.empty();
//        }
//        metadatas = metadatasOptional.get();
//
//        HDFSTable hdfsTable = new HDFSTable(table, tableLayout, columns, metadatas);
//        return Optional.of(hdfsTable);
//    }

    @Override
    public Optional<HDFSTableHandle> getTableHandle(String databaseName, String tableName)
    {
        log.debug("Get table handle " + Utils.formName(databaseName, tableName));
        HDFSTableHandle table;
        List<JDBCRecord> records;
        String sql = "SELECT tbl_name, tbl_location_uri FROM tbls WHERE tbl_name='"
                + Utils.formName(databaseName, tableName)
                + "';";
        String[] fields = {"tbl_name", "tbl_location_uri"};
        records = jdbcDriver.executreQuery(sql, fields);
        if (records.size() != 1) {
            log.error("Match more/less than one table");
            return Optional.empty();
        }
        JDBCRecord record = records.get(0);
        String schema = record.getString(fields[0]);
        String location = record.getString(fields[1]);
        table = new HDFSTableHandle(requireNonNull(Utils.getDatabaseName(schema), "database name is null"),
                requireNonNull(Utils.getTableName(schema), "table name is null"),
                requireNonNull(new Path(location), "location uri is null"));
        return Optional.of(table);
    }

    @Override
    public Optional<HDFSTableLayoutHandle> getTableLayout(String databaseName, String tableName)
    {
        log.debug("Get table layout " + Utils.formName(databaseName, tableName));
        HDFSTableLayoutHandle tableLayout;
        List<JDBCRecord> records;
        String sql = "SELECT fiber_col, time_col, fiber_func FROM tbl_params WHERE tbl_name='"
                + Utils.formName(databaseName, tableName)
                + "';";
        String[] fields = {"fiber_col", "time_col", "fiber_func"};
        records = jdbcDriver.executreQuery(sql, fields);
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

        tableLayout = new HDFSTableLayoutHandle(new SchemaTableName(databaseName, tableName), fiberCol, timeCol, fiberFunc);
        return Optional.of(tableLayout);
    }

    /**
     * Get all column handles of specified table
     * */
    public Optional<List<HDFSColumnHandle>> getTableColumnHandle(String databaseName, String tableName)
    {
        log.debug("Get list of column handles of table " + Utils.formName(databaseName, tableName));
        List<HDFSColumnHandle> columnHandles = new ArrayList<>();
        List<JDBCRecord> records;
        String colName;
        String databaseTableName = Utils.formName(databaseName, tableName);
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
            colName = Utils.formName(databaseName, tableName, record.getString(colFields[0]));
            columnHandles.add(getColumnHandle(colName));
        }
        return Optional.of(columnHandles);
    }

    private HDFSColumnHandle getColumnHandle(String databaseTableColName)
    {
        log.debug("Get handle of column " + databaseTableColName);
        String databaseName = Utils.getDatabaseName(databaseTableColName);
        String tableName = Utils.getTableName(databaseTableColName);
        String colName = Utils.getColName(databaseTableColName);
        return getColumnHandle(colName, Utils.formName(databaseName, tableName, colName));
    }

    private HDFSColumnHandle getColumnHandle(String colName, String databaseTableColName)
    {
        List<JDBCRecord> records;
        String sql = "SELECT col_type, type FROM cols WHERE col_name='"
                + databaseTableColName
                + "';";
        String[] colFields = {"col_type", "type"};
        records = jdbcDriver.executreQuery(sql, colFields);
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
        log.debug("Get list of column metadata of table " + Utils.formName(databaseName, tableName));
        List<ColumnMetadata> colMetadatas = new ArrayList<>();
        List<JDBCRecord> records;
        String tblName = Utils.formName(databaseName, tableName);
        String sql = "SELECT col_name FROM cols WHERE tbl_name='"
                + tblName
                + "';";
        String[] colFields = {"col_name"};
        records = jdbcDriver.executreQuery(sql, colFields);
        if (records.size() == 0) {
            log.warn("No col matches!");
            return Optional.empty();
        }
        records.forEach(record -> colMetadatas.add(getColMetadata(record.getString(colFields[0]))));
        return Optional.of(colMetadatas);
    }

    private ColumnMetadata getColMetadata(String databaseTableColName)
    {
        log.debug("Get metadata of col " + databaseTableColName);
        String colName = Utils.getColName(databaseTableColName);
        List<JDBCRecord> records;
        String sql = "SELECT type FROM cols WHERE col_name='"
                + databaseTableColName
                + "';";
        String[] colFields = {"type"};
        records = jdbcDriver.executreQuery(sql, colFields);
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
        log.debug("Get col type " + typeName);
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
        log.debug("Get type " + typeName);
        // check if type is varchar(xx)
        Pattern vcpattern = Pattern.compile("varchar\\(\\s*(\\d+)\\s*\\)");
        Matcher vcmatcher = vcpattern.matcher(typeName);
        if (vcmatcher.find()) {
            String vlen = vcmatcher.group(1);
            if (!vlen.isEmpty()) {
                return VarcharType.createVarcharType(Integer.parseInt(vlen));
            }
            return UnknownType.UNKNOWN;
        }
        // check if type is char(xx)
        Pattern cpattern = Pattern.compile("char\\(\\s*(\\d+)\\s*\\)");
        Matcher cmatcher = cpattern.matcher(typeName);
        if (cmatcher.find()) {
            String clen = cmatcher.group(1);
            if (!clen.isEmpty()) {
                return CharType.createCharType(Integer.parseInt(clen));
            }
            return UnknownType.UNKNOWN;
        }
        // check if type is decimal(precision, scale)
        Pattern dpattern = Pattern.compile("decimal\\((\\d+)\\s*,?\\s*(\\d*)\\)");
        Matcher dmatcher = dpattern.matcher(typeName);
        if (dmatcher.find()) {
            String dprecision = dmatcher.group(1);
            String dscale = dmatcher.group(2);
            if (dprecision.isEmpty()) {
                return UnknownType.UNKNOWN;
            }
            if (dscale.isEmpty()) {
                return DecimalType.createDecimalType(Integer.parseInt(dprecision));
            }
            return DecimalType.createDecimalType(Integer.parseInt(dprecision), Integer.parseInt(dscale));
        }
        // TODO add parameter info to types like decimal, etc.
        switch (typeName.toLowerCase()) {
            case "boolean": return BooleanType.BOOLEAN;
            case "char": return CharType.createCharType(CharType.MAX_LENGTH);
            case "tinyint": return TinyintType.TINYINT;
            case "smallint": return SmallintType.SMALLINT;
            case "integer": return IntegerType.INTEGER;
            case "bigint": return BigintType.BIGINT;
            case "real": return RealType.REAL;
            case "decimal": return DecimalType.createDecimalType();
            case "double": return DoubleType.DOUBLE;
            case "date": return DateType.DATE;
            case "time": return TimeType.TIME;
            case "timestamp": return TimestampType.TIMESTAMP;
            case "varchar": return VarcharType.VARCHAR;
            default: return UnknownType.UNKNOWN;
        }
    }

    @Override
    public void createDatabase(ConnectorSession session, HDFSDatabase database)
    {
        createDatabase(database);
    }

    private void createDatabase(HDFSDatabase database)
    {
        log.debug("Create database " + database.getName());
        createDatabase(database.getName(),
                database.getComment(),
                database.getLocation(),
                database.getOwner());
    }

    private void createDatabase(String dbName, String dbDesc, String dbPath, String owner)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO dbs(db_name, db_desc, db_location_uri, db_owner) VALUES('")
                .append(dbName)
                .append("', '")
                .append(dbDesc)
                .append("', '")
                .append(dbPath)
                .append("', '")
                .append(owner)
                .append("');");
        if (jdbcDriver.executeUpdate(sql.toString()) != 0) {
            // create hdfs dir
            try {
                log.debug("Sql: " + sql.toString());
                log.debug("Create hdfs dir at " + dbPath);
                fileSystem.mkdirs(new Path(dbPath));
            }
            catch (IOException e) {
                log.error(e);
            }
        }
        else {
            log.error("Create database" + dbName + " failed!");
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        log.debug("Create table " + tableMetadata.getTable().getTableName());
        String tableName = tableMetadata.getTable().getTableName();
        String schemaName = tableMetadata.getTable().getSchemaName();
        List<ColumnMetadata> colums = tableMetadata.getColumns();

        // get some more properties
        String location = Utils.formPath(schemaName, tableName).toString();
//        HDFSColumnHandle fiberCol = (HDFSColumnHandle) tableMetadata.getProperties().getOrDefault("fiber_col", "");
//        HDFSColumnHandle timeCol = (HDFSColumnHandle) tableMetadata.getProperties().getOrDefault("time_col", "");
        String fiberCol = "";
        String timeCol = "";
        String fiberFunc = (String) tableMetadata.getProperties().getOrDefault("fiber_func", "");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO tbls(db_name, tbl_name, tbl_location_uri) VALUES('")
                .append(schemaName)
                .append("', '")
                .append(Utils.formName(schemaName, tableName))
                .append("', '")
                .append(location)
                .append("');")
                .append("INSERT INTO tbl_params(tbl_name, fiber_col, time_col, fiber_func) VALUES('")
                .append(Utils.formName(schemaName, tableName))
                .append("', '")
                .append(fiberCol)
                .append("', '")
                .append(timeCol)
                .append("', '")
                .append(fiberFunc)
                .append("');");
        if (jdbcDriver.executeUpdate(sql.toString()) != 0) {
            try {
                log.debug("Create hdfs dir for " + Utils.formName(schemaName, tableName));
                fileSystem.mkdirs(Utils.formPath(schemaName, tableName));
            }
            catch (IOException e) {
                log.debug("Error sql: " + sql.toString());
                log.error(e);
                // TODO exit ?
            }
        }
        else {
            log.error("Create table " + Utils.formName(schemaName, tableName) + " failed!");
        }

        // add cols information
        for (ColumnMetadata col : colums) {
            sql.delete(0, sql.length() - 1);
            sql.append("INSERT INTO cols(tbl_name, col_name, type) VALUES('")
                    .append(Utils.formName(schemaName, tableName))
                    .append("', '")
                    .append(Utils.formName(schemaName, tableName, col.getName()))
                    .append("', '")
                    .append(col.getType())
                    .append("');");
            if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
                log.debug("Error sql: " + sql.toString());
                log.error("Create cols for table " + Utils.formName(schemaName, tableName) + " failed!");
                // TODO exit ?
            }
        }
    }
}
