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
    public void createTable(ConnectorSession session, ConnectorTableMetadata table)
    {
    }

    @Override
    public void createTableWithFiber(ConnectorSession session, ConnectorTableMetadata tableMetadata, String fiberKey, String function, String timeKey)
    {
    }

    @Override
    public void shutdown()
    {
    }
}
