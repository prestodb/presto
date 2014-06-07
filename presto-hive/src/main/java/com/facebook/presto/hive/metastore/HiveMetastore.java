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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.weakref.jmx.Managed;

import java.util.List;
import java.util.Map;

public interface HiveMetastore
{
    void createTable(Table table);

    void dropTable(String databaseName, String tableName);

    void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName);

    @Managed
    void flushCache();

    List<String> getAllDatabases();

    List<String> getAllTables(String databaseName)
            throws NoSuchObjectException;

    List<String> getAllViews(String databaseName)
            throws NoSuchObjectException;

    Database getDatabase(String databaseName)
            throws NoSuchObjectException;

    List<String> getPartitionNames(String databaseName, String tableName)
            throws NoSuchObjectException;

    List<String> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
            throws NoSuchObjectException;

    Map<String, Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException;

    Table getTable(String databaseName, String tableName)
            throws NoSuchObjectException;
}
