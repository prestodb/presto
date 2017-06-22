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
import com.facebook.presto.hdfs.HDFSDatabase;
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.hdfs.HDFSTableLayoutHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Optional;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public interface MetaServer
{
    List<String> getAllDatabases();

//    public Optional<HDFSDatabase> getDatabase(String databaseName);

//    public Optional<List<String>> getAllTables(String databaseName);

    List<SchemaTableName> listTables(SchemaTablePrefix prefix);

//    public Optional<HDFSTable> getTable(String databaseName, String tableName);

    Optional<HDFSTableHandle> getTableHandle(String connectorId, String databaseName, String tableName);

    Optional<HDFSTableLayoutHandle> getTableLayout(String connectorId, String databaseName, String tableName);

    Optional<List<ColumnMetadata>> getTableColMetadata(String connectorId, String databaseName, String tableName);

    Optional<List<HDFSColumnHandle>> getTableColumnHandle(String connectorId, String databaseName, String tableName);

    void createDatabase(ConnectorSession session, HDFSDatabase database);

    void deleteDatabase(String database);

    void renameDatabase(String oldName, String newName);

    void createTable(ConnectorSession session, ConnectorTableMetadata table);

    void deleteTable(String database, String table);

    void renameTable(String database, String oldTable, String newTable);

    void renameColumn(String database, String table, String oldCol, String newCol);

    void createTableWithFiber(ConnectorSession session, ConnectorTableMetadata tableMetadata, String fiberKey, String function, String timeKey);

    void createFiber(String database, String table, long value);

    void updateFiber(String database, String table, long oldV, long newV);

    void deleteFiber(String database, String table, long value);

    List<Long> getFibers(String database, String table);

    void addBlock(long fiberId, String timeB, String timeE, String path);

    void deleteBlock(long fiberId, String path);

    List<String> getBlocks(long fiberId, String timeB, String timeE);

    List<String> filterBlocks(String db, String table, Optional<Long> fiberId, Optional<Long> timeLow, Optional<Long> timeHigh);

    void shutdown();
}
