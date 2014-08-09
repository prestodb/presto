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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryHiveMetastore
        implements HiveMetastore
{
    private final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SchemaTableName, Table> relations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SchemaTableName, Table> views = new ConcurrentHashMap<>();

    public void createDatabase(Database database)
    {
        checkNotNull(database, "database is null");

        File file = new File(URI.create(database.getLocationUri()));
        file.mkdirs();

        if (databases.putIfAbsent(database.getName(), database) != null) {
            throw new IllegalArgumentException("Database " + database.getName() + " already exists");
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public void createTable(Table table)
    {
        SchemaTableName schemaTableName = new SchemaTableName(table.getDbName(), table.getTableName());
        Table tableCopy = table.deepCopy();
        if (tableCopy.getSd() == null) {
            tableCopy.setSd(new StorageDescriptor());
        }

        if (relations.putIfAbsent(schemaTableName, tableCopy) != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        if (tableCopy.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            views.put(schemaTableName, tableCopy);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        if (relations.remove(schemaTableName) == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        views.remove(schemaTableName);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        // TODO: use locking to do this properly
        SchemaTableName oldTable = new SchemaTableName(databaseName, tableName);
        Table table = relations.get(oldTable);
        if (table == null) {
            throw new TableNotFoundException(oldTable);
        }

        SchemaTableName newTable = new SchemaTableName(newDatabaseName, newTableName);
        if (relations.putIfAbsent(newTable, table) != null) {
            throw new TableAlreadyExistsException(newTable);
        }
        relations.remove(oldTable);
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws NoSuchObjectException
    {
        ImmutableList.Builder<String> tables = ImmutableList.builder();
        for (SchemaTableName schemaTableName : this.relations.keySet()) {
            if (schemaTableName.getSchemaName().equals(databaseName)) {
                tables.add(schemaTableName.getTableName());
            }
        }
        return tables.build();
    }

    @Override
    public List<String> getAllViews(String databaseName)
            throws NoSuchObjectException
    {
        ImmutableList.Builder<String> tables = ImmutableList.builder();
        for (SchemaTableName schemaTableName : this.views.keySet()) {
            if (schemaTableName.getSchemaName().equals(databaseName)) {
                tables.add(schemaTableName.getTableName());
            }
        }
        return tables.build();
    }

    @Override
    public Database getDatabase(String databaseName)
            throws NoSuchObjectException
    {
        Database database = databases.get(databaseName);
        if (database == null) {
            throw new NoSuchObjectException();
        }
        return database;
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
            throws NoSuchObjectException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = relations.get(schemaTableName);
        if (table == null) {
            throw new NoSuchObjectException();
        }
        return table;
    }

    @Override
    public void flushCache()
    {
    }
}
