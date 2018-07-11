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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiPartition;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiPrivilegeGrantInfo;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

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
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName).map(ThriftMetastoreUtil::fromMetastoreApiDatabase);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName).map(ThriftMetastoreUtil::fromMetastoreApiTable);
    }

    @Override
    public boolean supportsColumnStatistics()
    {
        return delegate.supportsColumnStatistics();
    }

    @Override
    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return delegate.getTableStatistics(databaseName, tableName);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        return delegate.getPartitionStatistics(databaseName, tableName, partitionNames);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updateTableStatistics(databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updatePartitionStatistics(databaseName, tableName, partitionName, update);
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
    public void createDatabase(Database database)
    {
        delegate.createDatabase(toMetastoreApiDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        delegate.dropDatabase(databaseName);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        org.apache.hadoop.hive.metastore.api.Database database = delegate.getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
        database.setName(newDatabaseName);
        delegate.alterDatabase(databaseName, database);

        delegate.getDatabase(databaseName).ifPresent(newDatabase -> {
            if (newDatabase.getName().equals(databaseName)) {
                throw new PrestoException(NOT_SUPPORTED, "Hive metastore does not support renaming schemas");
            }
        });
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(toMetastoreApiTable(table, principalPrivileges));
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        alterTable(databaseName, tableName, toMetastoreApiTable(newTable, principalPrivileges));
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
    public synchronized void updateTableParameters(String databaseName, String tableName, Function<Map<String, String>, Map<String, String>> update)
    {
        org.apache.hadoop.hive.metastore.api.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        Map<String, String> parameters = table.getParameters();
        Map<String, String> updatedParameters = requireNonNull(update.apply(parameters), "updatedParameters is null");
        if (!parameters.equals(updatedParameters)) {
            table.setParameters(updatedParameters);
            alterTable(databaseName, tableName, table);
        }
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
                new FieldSchema(columnName, columnType.getHiveTypeName().toString(), columnComment));
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
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
            }
        }
        for (FieldSchema fieldSchema : table.getSd().getCols()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                fieldSchema.setName(newColumnName);
            }
        }
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, databaseName, tableName, columnName);
        org.apache.hadoop.hive.metastore.api.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.getSd().getCols().removeIf(fieldSchema -> fieldSchema.getName().equals(columnName));
        alterTable(databaseName, tableName, table);
    }

    private void alterTable(String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table)
    {
        delegate.alterTable(databaseName, tableName, table);
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        return delegate.getPartition(databaseName, tableName, partitionValues).map(ThriftMetastoreUtil::fromMetastoreApiPartition);
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
                .map(ThriftMetastoreUtil::fromMetastoreApiPartition)
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
                        .map(ThriftMetastoreUtil::toMetastoreApiPartition)
                        .collect(Collectors.toList()));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        delegate.alterPartition(databaseName, tableName, toMetastoreApiPartition(partition));
    }

    @Override
    public synchronized void updatePartitionParameters(String databaseName, String tableName, List<String> partitionValues, Function<Map<String, String>, Map<String, String>> update)
    {
        org.apache.hadoop.hive.metastore.api.Partition partition = delegate.getPartition(databaseName, tableName, partitionValues)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
        Map<String, String> parameters = partition.getParameters();
        Map<String, String> updatedParameters = requireNonNull(update.apply(parameters), "updatedParameters is null");
        if (!parameters.equals(updatedParameters)) {
            partition.setParameters(updatedParameters);
            delegate.alterPartition(databaseName, tableName, partition);
        }
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
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> privilegeGrantInfos = privileges.stream()
                .map(privilege -> toMetastoreApiPrivilegeGrantInfo(grantee, privilege))
                .collect(Collectors.toSet());
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privilegeGrantInfos);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> privilegeGrantInfos = privileges.stream()
                .map(privilege -> toMetastoreApiPrivilegeGrantInfo(grantee, privilege))
                .collect(Collectors.toSet());
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privilegeGrantInfos);
    }
}
