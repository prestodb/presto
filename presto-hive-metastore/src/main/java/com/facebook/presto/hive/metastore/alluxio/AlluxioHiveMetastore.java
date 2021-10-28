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
package com.facebook.presto.hive.metastore.alluxio;

import alluxio.client.table.TableMasterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of the {@link HiveMetastore} interface through Alluxio.
 */
public class AlluxioHiveMetastore
        implements ExtendedHiveMetastore
{
    private final TableMasterClient client;

    @Inject
    public AlluxioHiveMetastore(TableMasterClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            return Optional.of(AlluxioProtoUtils.fromProto(client.getDatabase(databaseName)));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        try {
            return client.getAllDatabases();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            return Optional.of(AlluxioProtoUtils.fromProto(client.getTable(databaseName, tableName)));
        }
        catch (NotFoundException e) {
            return Optional.empty();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    private Map<String, HiveColumnStatistics> groupStatisticsByColumn(MetastoreContext metastoreContext, List<ColumnStatisticsInfo> statistics, OptionalLong rowCount)
    {
        return statistics.stream()
                .collect(toImmutableMap(ColumnStatisticsInfo::getColName, statisticsInfo -> AlluxioProtoUtils.fromProto(statisticsInfo.getData(), rowCount)));
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            Table table = getTable(metastoreContext, databaseName, tableName).orElseThrow(
                    () -> new PrestoException(HIVE_METASTORE_ERROR, String.format("Could not retrieve table %s.%s", databaseName, tableName)));
            HiveBasicStatistics basicStatistics = getHiveBasicStatistics(table.getParameters());
            List<Column> columns = table.getPartitionColumns();
            List<String> columnNames = columns.stream().map(Column::getName).collect(toImmutableList());
            List<ColumnStatisticsInfo> columnStatistics = client.getTableColumnStatistics(table.getDatabaseName(), table.getTableName(), columnNames);
            return new PartitionStatistics(basicStatistics, groupStatisticsByColumn(metastoreContext, columnStatistics, basicStatistics.getRowCount()));
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getTable(metastoreContext, databaseName, tableName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        Map<String, HiveBasicStatistics> partitionBasicStatistics = getPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.copyOf(partitionNames)).entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(
                        entry -> MetastoreUtil.makePartName(table.getPartitionColumns(), entry.getValue().get().getValues()),
                        entry -> getHiveBasicStatistics(entry.getValue().get().getParameters())));

        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));

        List<String> dataColumns = table.getDataColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, List<ColumnStatisticsInfo>> columnStatisticss;
        try {
            columnStatisticss = client.getPartitionColumnStatistics(
                    table.getDatabaseName(),
                    table.getTableName(),
                    partitionBasicStatistics.keySet().stream().collect(toImmutableList()),
                    dataColumns);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = columnStatisticss.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(metastoreContext, entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));

        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (String partitionName : partitionBasicStatistics.keySet()) {
            HiveBasicStatistics basicStatistics = partitionBasicStatistics.get(partitionName);
            Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStatistics.getOrDefault(partitionName, ImmutableMap.of());
            result.put(partitionName, new PartitionStatistics(basicStatistics, columnStatistics));
        }
        return result.build();
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updateTableStatistics is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updatePartitionStatistics is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            return Optional.of(client.getAllTables(databaseName));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        // TODO: Add views on the server side
        return Optional.of(Collections.emptyList());
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        throw new UnsupportedOperationException("createDatabase is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        throw new UnsupportedOperationException("dropDatabase is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException("renameDatabase is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("createTable is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropTable is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("replaceTable is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException("renameTable is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("addColumn is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("renameColumn is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("dropColumn is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("getPartition is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            List<PartitionInfo> partitionInfos = AlluxioProtoUtils.toPartitionInfoList(client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            return Optional.of(partitionInfos.stream().map(PartitionInfo::getPartitionName).collect(toImmutableList()));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getPartitionNamesByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        List<String> parts = convertPredicateToParts(partitionPredicates);
        return getPartitionNamesByParts(metastoreContext, databaseName, tableName, parts).orElse(ImmutableList.of());
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * return a list of partition names by which the values of each partition is at least
     * contained which the {@code parts} argument
     *
     * @param databaseName database name
     * @param tableName    table name
     * @param parts        list of values which returned partitions should contain
     * @return optionally, a list of strings where each entry is in the form of {key}={value}
     */
    public Optional<List<String>> getPartitionNamesByParts(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts)
    {
        try {
            List<PartitionInfo> partitionInfos = AlluxioProtoUtils.toPartitionInfoList(client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));

            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName))
                    // Filter out any partitions which have values that don't match
                    .filter(partition -> {
                        List<String> values = partition.getValuesList();
                        if (values.size() != parts.size()) {
                            return false;
                        }
                        for (int i = 0; i < values.size(); i++) {
                            String constraintPart = parts.get(i);
                            if (!constraintPart.isEmpty() && !values.get(i).equals(constraintPart)) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .collect(toImmutableList());
            List<String> partitionNames = partitionInfos.stream().map(PartitionInfo::getPartitionName).collect(toImmutableList());
            return Optional.of(partitionNames);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        try {
            // Get all partitions
            List<PartitionInfo> partitionInfos = AlluxioProtoUtils.toPartitionInfoList(client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));

            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName)).collect(toImmutableList());
            return partitionInfos.stream()
                    .filter(p -> partitionNames.stream().anyMatch(p.getPartitionName()::equals))
                    .collect(toImmutableMap(PartitionInfo::getPartitionName, partitionInfo -> Optional.of(AlluxioProtoUtils.fromProto(partitionInfo))));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("addPartitions is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropPartition is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("alterPartition is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        throw new UnsupportedOperationException("createRole is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        throw new UnsupportedOperationException("dropRole is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        throw new UnsupportedOperationException("listRoles is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("grantRoles is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("revokeRoles is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("listRoleGrants is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("grantTablePrivileges is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("revokeTablePrivileges is not supported in AlluxioHiveMetastore");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("listTablePrivileges is not supported in AlluxioHiveMetastore");
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new UnsupportedOperationException("setPartitionLeases is not supported in AlluxioHiveMetastore");
    }
}
