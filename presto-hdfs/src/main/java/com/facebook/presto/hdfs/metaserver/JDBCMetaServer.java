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

import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.hdfs.HDFSDatabase;
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.hdfs.jdbc.JDBCDriver;
import com.facebook.presto.hdfs.jdbc.JDBCRecord;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
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
        // COL_NAME: TBL_NAME.col_name
        sqlTable.putIfAbsent("cols",
                "CREATE TABLE COLS(COL_ID BIGSERIAL PRIMARY KEY, TBL_NAME varchar(256), COL_NAME varchar(384) UNIQUE, COL_TYPE int);");
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

    @Override
    public Optional<HDFSDatabase> getDatabase(String databaseName)
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

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
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
        return null;
    }

    @Override
    public Optional<HDFSTableHandle> getTable(String databaseName, String tableName)
    {
        HDFSTableHandle table;
        List<JDBCRecord> records;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT tbl_name, tbl_location_uri FROM tbls WHERE tbl_name='")
                .append(databaseName)
                .append(".")
                .append(tableName)
                .append("'");
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

    @Override
    public boolean isDatabaseEmpty(ConnectorSession session, String databaseName)
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
    public void createTable(ConnectorSession session, HDFSTableHandle table)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO tbls(db_name, tbl_name, tbl_location_uri) VALUES('")
                .append(table.getSchemaName())
                .append("', '")
                .append(table.getSchemaTableName().toString())
                .append("', '")
                .append(table.getLocation())
                .append("');");
        if (jdbcDriver.executeUpdate(sql.toString()) == 0) {
            log.error("Create table " + table.getSchemaName().toString() + " failed!");
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
