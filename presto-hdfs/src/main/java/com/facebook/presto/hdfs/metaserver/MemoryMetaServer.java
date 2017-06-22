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
public class MemoryMetaServer
    implements MetaServer
{
    @Override
    public List<String> getAllDatabases()
    {
        return null;
    }

    @Override
    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        return null;
    }

    @Override
    public Optional<HDFSTableHandle> getTableHandle(String connectorId, String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public Optional<HDFSTableLayoutHandle> getTableLayout(String connectorId, String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public Optional<List<ColumnMetadata>> getTableColMetadata(String connectorId, String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public Optional<List<HDFSColumnHandle>> getTableColumnHandle(String connectorId, String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public void createDatabase(ConnectorSession session, HDFSDatabase database)
    {
    }

    @Override
    public void deleteDatabase(String database)
    {
    }

    @Override
    public void renameDatabase(String oldName, String newName)
    {
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata table)
    {
    }

    @Override
    public void deleteTable(String database, String table)
    {
    }

    @Override
    public void renameTable(String database, String oldTable, String newTable)
    {
    }

    @Override
    public void renameColumn(String database, String table, String oldCol, String newCol)
    {
    }

    @Override
    public void createTableWithFiber(ConnectorSession session, ConnectorTableMetadata tableMetadata, String fiberKey, String function, String timeKey)
    {
    }

    @Override
    public void createFiber(String database, String table, long value)
    {
    }

    @Override
    public void updateFiber(String database, String table, long oldV, long newV)
    {
    }

    @Override
    public void deleteFiber(String database, String table, long value)
    {
    }

    @Override
    public List<Long> getFibers(String database, String table)
    {
        return null;
    }

    @Override
    public void addBlock(long fiberId, String timeB, String timeE, String path)
    {
    }

    @Override
    public void deleteBlock(long fiberId, String path)
    {
    }

    @Override
    public List<String> getBlocks(long fiberId, String timeB, String timeE)
    {
        return null;
    }

    @Override
    public List<String> filterBlocks(String db, String table, Optional<Long> fiberId, Optional<Long> timeLow, Optional<Long> timeHigh)
    {
        return null;
    }

    @Override
    public void shutdown()
    {
    }
}
