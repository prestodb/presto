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

import com.facebook.presto.hdfs.HDFSDatabase;
import com.facebook.presto.hdfs.HDFSTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Optional;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class JDBCMetaServer
implements MetaServer
{
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
