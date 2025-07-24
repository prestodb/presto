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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import jakarta.inject.Inject;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNamesWithEmptyVersion;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiPartition;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.isAvroTableWithSchemaSet;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.isCsvTable;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class BridgingHiveMetastore
        implements ExtendedHiveMetastore
{
    private final HiveMetastore delegate;
    private final PartitionMutator partitionMutator;

    @Inject
    public BridgingHiveMetastore(HiveMetastore delegate, PartitionMutator partitionMutator)
    {
        this.delegate = delegate;
        this.partitionMutator = partitionMutator;
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return delegate.getDatabase(metastoreContext, databaseName).map(ThriftMetastoreUtil::fromMetastoreApiDatabase);
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return delegate.getAllDatabases(metastoreContext);
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, new HiveTableHandle(databaseName, tableName));
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        return delegate.getTable(metastoreContext, hiveTableHandle).map(table -> {
            if (isAvroTableWithSchemaSet(table) || isCsvTable(table)) {
                return fromMetastoreApiTable(table, delegate.getFields(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName()).get(), metastoreContext.getColumnConverter());
            }
            return fromMetastoreApiTable(table, metastoreContext.getColumnConverter());
        });
    }

    @Override
    public List<TableConstraint<String>> getTableConstraints(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        ImmutableList.Builder<TableConstraint<String>> constraints = ImmutableList.builder();
        Optional<PrimaryKeyConstraint<String>> primaryKey = delegate.getPrimaryKey(metastoreContext, databaseName, tableName);
        if (primaryKey.isPresent()) {
            constraints.add(primaryKey.get());
        }
        constraints.addAll(delegate.getUniqueConstraints(metastoreContext, databaseName, tableName));
        constraints.addAll(delegate.getNotNullConstraints(metastoreContext, databaseName, tableName));
        return constraints.build();
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return delegate.getSupportedColumnStatistics(metastoreContext, type);
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return delegate.getTableStatistics(metastoreContext, databaseName, tableName);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        return delegate.getPartitionStatistics(metastoreContext, databaseName, tableName, partitionNames);
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updateTableStatistics(metastoreContext, databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updatePartitionStatistics(metastoreContext, databaseName, tableName, partitionName, update);
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        return delegate.getAllTables(metastoreContext, databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        return delegate.getAllViews(metastoreContext, databaseName);
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        delegate.createDatabase(metastoreContext, toMetastoreApiDatabase(database));
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        delegate.dropDatabase(metastoreContext, databaseName);
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        org.apache.hadoop.hive.metastore.api.Database database = delegate.getDatabase(metastoreContext, databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
        database.setName(newDatabaseName);
        delegate.alterDatabase(metastoreContext, databaseName, database);

        delegate.getDatabase(metastoreContext, databaseName).ifPresent(newDatabase -> {
            if (newDatabase.getName().equals(databaseName)) {
                throw new PrestoException(NOT_SUPPORTED, "Hive metastore does not support renaming schemas");
            }
        });
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        checkArgument(!table.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");
        return delegate.createTable(metastoreContext, toMetastoreApiTable(table, principalPrivileges, metastoreContext.getColumnConverter()), constraints);
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(metastoreContext, databaseName, tableName, deleteData);
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        checkArgument(!newTable.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");
        return alterTable(metastoreContext, databaseName, tableName, toMetastoreApiTable(newTable, principalPrivileges, metastoreContext.getColumnConverter()));
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(metastoreContext, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        table.setDbName(newDatabaseName);
        table.setTableName(newTableName);
        return alterTable(metastoreContext, databaseName, tableName, table);
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(metastoreContext, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        Column column = new Column(columnName, columnType, Optional.ofNullable(columnComment), Optional.empty());
        table.getSd().getCols().add(metastoreContext.getColumnConverter().fromColumn(column));
        return alterTable(metastoreContext, databaseName, tableName, table);
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = delegate.getTable(metastoreContext, databaseName, tableName);
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
        return alterTable(metastoreContext, databaseName, tableName, table);
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, metastoreContext, databaseName, tableName, columnName);
        org.apache.hadoop.hive.metastore.api.Table table = delegate.getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.getSd().getCols().removeIf(fieldSchema -> fieldSchema.getName().equals(columnName));
        return alterTable(metastoreContext, databaseName, tableName, table);
    }

    private MetastoreOperationResult alterTable(MetastoreContext metastoreContext, String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table)
    {
        return delegate.alterTable(metastoreContext, databaseName, tableName, table);
    }

    private MetastoreOperationResult alterTableWithEnvironmentContext(MetastoreContext metastoreContext, String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table, EnvironmentContext environmentContext)
    {
        return delegate.alterTableWithEnvironmentContext(metastoreContext, databaseName, tableName, table, environmentContext);
    }

    @Override
    public MetastoreOperationResult persistTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Supplier<PartitionStatistics> update, Map<String, String> additionalParameters)
    {
        checkArgument(!newTable.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");
        Map<String, String> env = Maps.newHashMapWithExpectedSize(additionalParameters.size() + 1);
        env.putAll(additionalParameters);
        env.put(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
        return alterTableWithEnvironmentContext(metastoreContext, databaseName, tableName, toMetastoreApiTable(newTable, principalPrivileges, metastoreContext.getColumnConverter()), new EnvironmentContext(env));
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        return delegate.getPartition(metastoreContext, databaseName, tableName, partitionValues).map(partition -> fromMetastoreApiPartition(partition, partitionMutator, metastoreContext.getColumnConverter()));
    }

    @Override
    public Optional<List<PartitionNameWithVersion>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return delegate.getPartitionNames(metastoreContext, databaseName, tableName).map(MetastoreUtil::getPartitionNamesWithEmptyVersion);
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return getPartitionNamesWithEmptyVersion(delegate.getPartitionNamesByFilter(metastoreContext, databaseName, tableName, partitionPredicates));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNamesWithVersion)
    {
        requireNonNull(partitionNamesWithVersion, "partitionNames is null");
        if (partitionNamesWithVersion.isEmpty()) {
            return ImmutableMap.of();
        }

        List<String> partitionNames = partitionNamesWithVersion.stream()
                .map(PartitionNameWithVersion::getPartitionName)
                .collect(toImmutableList());
        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(Collectors.toMap(identity(), MetastoreUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = delegate.getPartitionsByNames(metastoreContext, databaseName, tableName, partitionNames).stream()
                .map(partition -> fromMetastoreApiPartition(partition, partitionMutator, metastoreContext.getColumnConverter()))
                .collect(Collectors.toMap(Partition::getValues, identity()));
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        return delegate.addPartitions(metastoreContext, databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        return delegate.alterPartition(metastoreContext, databaseName, tableName, partition);
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        delegate.createRole(metastoreContext, role, grantor);
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        delegate.dropRole(metastoreContext, role);
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return delegate.listRoles(metastoreContext);
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        delegate.grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        delegate.revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(metastoreContext, principal);
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.grantTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.revokeTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        return delegate.listTablePrivileges(metastoreContext, databaseName, tableName, principal);
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        delegate.setPartitionLeases(metastoreContext, databaseName, tableName, partitionNameToLocation, leaseDuration);
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        return delegate.dropConstraint(metastoreContext, databaseName, tableName, constraintName);
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        MetastoreOperationResult result;
        if (tableConstraint instanceof UniqueConstraint || tableConstraint instanceof PrimaryKeyConstraint || tableConstraint instanceof NotNullConstraint) {
            result = delegate.addConstraint(metastoreContext, databaseName, tableName, tableConstraint);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Hive metastore supports only unique/primary key/not null constraints");
        }
        return result;
    }

    @Override
    public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return Optional.of(delegate.lock(metastoreContext, databaseName, tableName));
    }

    @Override
    public void unlock(MetastoreContext metastoreContext, long lockId)
    {
        delegate.unlock(metastoreContext, lockId);
    }
}
