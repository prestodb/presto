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
package com.facebook.presto.raptor.metadata;

import com.facebook.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public final class SchemaDaoUtil
{
    private static final Logger log = Logger.get(SchemaDaoUtil.class);

    private SchemaDaoUtil() {}

    public static void createTablesWithRetry(IDBI dbi)
    {
        Duration delay = new Duration(2, TimeUnit.SECONDS);
        while (true) {
            try (Handle handle = dbi.open()) {
                createTables(handle.attach(SchemaDao.class));
                alterTables(handle.getConnection(), handle.attach(SchemaDao.class));
                return;
            }
            catch (UnableToObtainConnectionException | SQLTransientException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                sleep(delay);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void createTables(SchemaDao dao)
    {
        dao.createTableDistributions();
        dao.createTableTables();
        dao.createTableColumns();
        dao.createTableViews();
        dao.createTableNodes();
        dao.createTableShards();
        dao.createTableShardNodes();
        dao.createTableExternalBatches();
        dao.createTableTransactions();
        dao.createTableCreatedShards();
        dao.createTableDeletedShards();
        dao.createTableBuckets();
        dao.createTableShardOrganizerJobs();
    }

    private static void alterTables(Connection connection, SchemaDao dao)
            throws SQLException
    {
        // for upgrading compatibility
        alterTable(dao::alterTableTablesWithDeltaDelete, "tables", "table_supports_delta_delete", connection);
        alterTable(dao::alterTableTablesWithDeltaCount, "tables", "delta_count", connection);
        alterTable(dao::alterTableShardsWithIsDelta, "shards", "is_delta", connection);
        alterTable(dao::alterTableShardsWithDeltaUuid, "shards", "delta_uuid", connection);
    }

    public static void alterTable(Runnable alterTableFunction, String tableName, String columnName, Connection connection)
            throws SQLException
    {
        if (!findColumnName(tableName, columnName, connection)) {
            log.info("Alter table %s, add column %s", tableName, columnName);
            alterTableFunction.run();
        }
    }

    public static boolean findColumnName(String tableName, String columnName, Connection connection)
            throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery("SELECT * FROM " + tableName + " LIMIT 0");
        ResultSetMetaData metadata = results.getMetaData();
        int columnCount = metadata.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (columnName.equalsIgnoreCase(metadata.getColumnName(i + 1))) {
                return true;
            }
        }
        return false;
    }

    public static void sleep(Duration duration)
    {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
