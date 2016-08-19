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

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.MetastoreUtil.toMetastoreApiTable;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class BridgingHiveMetastore
        implements ExtendedHiveMetastore
{
    private final HiveMetastore delegate;

    @Inject
    public BridgingHiveMetastore(HiveMetastore delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void flushCache()
    {
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName).map(MetastoreUtil::fromMetastoreApiTable);
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
    }

    @Override
    public void createTable(Table table, PrincipalPrivilegeSet principalPrivilegeSet)
    {
        delegate.createTable(toMetastoreApiTable(table, principalPrivilegeSet));
    }

    @Override
    public void dropTable(String databaseName, String tableName)
    {
        delegate.dropTable(databaseName, tableName);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivilegeSet principalPrivilegeSet)
    {
        alterTable(databaseName, tableName, toMetastoreApiTable(newTable, principalPrivilegeSet));
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        table.setDbName(newDatabaseName);
        table.setTableName(newTableName);
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        table.getSd().getCols().add(
                new FieldSchema(columnName, columnType.getHiveTypeName(), columnComment));
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        for (FieldSchema fieldSchema : table.getSd().getCols()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                fieldSchema.setName(newColumnName);
            }
        }
        alterTable(databaseName, tableName, table);
    }

    private void alterTable(String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table)
    {
        delegate.alterTable(databaseName, tableName, table);
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        return delegate.getPartition(databaseName, tableName, partitionValues).map(MetastoreUtil::fromMetastoreApiPartition);
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return delegate.getPartitionNames(databaseName, tableName);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return delegate.getPartitionNamesByParts(databaseName, tableName, parts);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(Collectors.toMap(identity(), HiveUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = delegate.getPartitionsByNames(databaseName, tableName, partitionNames).stream()
                .map(MetastoreUtil::fromMetastoreApiPartition)
                .collect(Collectors.toMap(Partition::getValues, identity()));
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        delegate.addPartitions(
                databaseName,
                tableName,
                partitions.stream()
                        .map(MetastoreUtil::toMetastoreApiPartition)
                        .collect(Collectors.toList()));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts)
    {
        delegate.dropPartition(databaseName, tableName, parts);
    }

    @Override
    public Set<String> getRoles(String user)
    {
        return delegate.getRoles(user);
    }

    @Override
    public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        return delegate.getDatabasePrivileges(user, databaseName);
    }

    @Override
    public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        return delegate.getTablePrivileges(user, databaseName, tableName);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privilegeGrantInfoSet);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privilegeGrantInfoSet);
    }
}
