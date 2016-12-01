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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import io.airlift.log.Logger;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
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

        sqlTable.putIfAbsent("DBS",
                "CREATE TABLE DBS(DB_ID BIGSERIAL PRIMARY KEY, DB_DESC varchar(4000), DB_NAME varchar(128) UNIQUE, DB_LOCATION_URI varchar(4000), DB_OWNER varchar(128));");
        sqlTable.putIfAbsent("TBLS",
                "CREATE TABLE TBLS(TBL_ID BIGSERIAL PRIMARY KEY, DB_ID bigint, TBL_NAME varchar(128) UNIQUE, TBL_LOCATION_URI varchar(4000));");
        sqlTable.putIfAbsent("TBL_PARAMS",
                "CREATE TABLE TBL_PARAMS(TBL_ID bigint, FIBER_COL bigint, TIME_COL bigint, FIBER_FUNC varchar(4000));");
        sqlTable.putIfAbsent("COLS",
                "CREATE TABLE COLS(COL_ID BIGSERIAL PRIMARY KEY, COL_NAME varchar(128), COL_TYPE int);");
        sqlTable.putIfAbsent("FIBERS",
                "CREATE TABLE FIBERS(INDEX_ID BIGSERIAL PRIMARY KEY, TBL_ID bigint, FIBER bigint, TIME_BEGIN timestamp, TIME_END timestamp);");

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
                    if (jdbcDriver.executeDDL(sqlTable.get(tbl)) != 0) {
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
        return null;
    }

    @Override
    public Optional<HDFSDatabase> getDatabase(String databaseName)
    {
        return null;
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return null;
    }

    @Override
    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        return null;
    }

    @Override
    public Optional<HDFSTableHandle> getTable(String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public void createDatabase(ConnectorSession session, HDFSDatabase database)
    {
    }

    @Override
    public boolean isDatabaseEmpty(ConnectorSession session, String databaseName)
    {
        return false;
    }

    @Override
    public void dropDatabase(ConnectorSession session, String databaseName)
    {
    }

    @Override
    public void renameDatabase(ConnectorSession session, String source, String target)
    {
    }

    @Override
    public void createTable(ConnectorSession session, HDFSTableHandle table)
    {
    }

    @Override
    public void dropTable(ConnectorSession session, String databaseName, String tableName)
    {
    }

    @Override
    public void renameTable(ConnectorSession session, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
    }

    @Override
    public void commit()
    {
    }

    @Override
    public void rollback()
    {
    }
}
