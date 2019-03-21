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

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.RetryDriver;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.ObjectNames;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.createMetastoreColumnStatistics;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiPrincipalType;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromPrestoPrincipalType;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromRolePrincipalGrants;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.parsePrivilege;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiPartition;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.api.HiveObjectType.TABLE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

@ThreadSafe
public class ThriftHiveMetastore
        implements HiveMetastore
{
    private final ThriftHiveMetastoreStats stats;
    private final MBeanExporter exporter;
    private final HiveCluster clientProvider;
    private final Function<Exception, Exception> exceptionMapper;
    private final Map<HostAndPort, ThriftHiveMetastoreStats> metastoreStats;
    private final boolean isHmsImpersonationEnabled;

    @Inject
    public ThriftHiveMetastore(HiveCluster hiveCluster, MetastoreClientConfig config, MBeanExporter exporter)
    {
        this(hiveCluster, config, exporter, new ThriftHiveMetastoreStats(), identity());
    }

    public ThriftHiveMetastore(HiveCluster hiveCluster, MetastoreClientConfig config, MBeanExporter exporter, ThriftHiveMetastoreStats stats, Function<Exception, Exception> exceptionMapper)
    {
        this.isHmsImpersonationEnabled = config.isMetastoreImpersonationEnabled();
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.exceptionMapper = requireNonNull(exceptionMapper, "exceptionMapper is null");
        ImmutableMap.Builder<HostAndPort, ThriftHiveMetastoreStats> metastoreStatsBuilder = ImmutableMap.builder();
        for (HostAndPort hostAndPort : hiveCluster.getAddresses()) {
            metastoreStatsBuilder.put(hostAndPort, new ThriftHiveMetastoreStats());
        }
        this.metastoreStats = metastoreStatsBuilder.build();
        exportMetastoreStatistics();
    }

    private void exportMetastoreStatistics()
    {
        for (HostAndPort key : metastoreStats.keySet()) {
            String name = ObjectNames.builder(ThriftHiveMetastore.class)
                    .withProperty("host", key.getHost())
                    .build();
            exporter.export(name, metastoreStats.get(key));
        }
    }

    private static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    @Managed
    @Flatten
    public ThriftHiveMetastoreStats getStats()
    {
        return stats;
    }

    @Managed
    @Flatten
    public List<ThriftHiveMetastoreStats> getAllStats()
    {
        return new ArrayList<>(metastoreStats.values());
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> stat.getGetAllDatabases().wrapCall(client::getAllDatabases))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> Optional.of(stat.getGetDatabase().wrapCall(() -> client.getDatabase(databaseName))))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(null)) {
                ThriftHiveMetastoreStats stat = metastoreStats.get(client.getAddress());
                return stat.getGetAllTables().wrap(() -> client.getAllTables(databaseName)).call();
            }
        });

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(null)) {
                ThriftHiveMetastoreStats stat = metastoreStats.get(client.getAddress());
                stat.getGetDatabase().wrap(() -> client.getDatabase(databaseName)).call();
                return null;
            }
        });

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            // Check to see if the database exists
                            getDatabase.call();
                        }
                        return Optional.of(tables);
                    });
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getGetTable().wrapCall(() -> {
                                        Table table = client.getTable(databaseName, tableName);
                                        if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && !isPrestoView(table)) {
                                            throw new HiveViewNotSupportedException(new SchemaTableName(databaseName, tableName));
                                        }
                                        return Optional.of(table);
                                    }))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(table.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = getTableColumnStatistics(databaseName, tableName, dataColumns, basicStatistics.getRowCount());
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, List<String> columns, OptionalLong rowCount)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTableColumnStatistics", stats.getGetTableColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    groupStatisticsByColumn(stat.getGetTableColumnStatistics().wrapCall(() -> client.getTableColumnStatistics(databaseName, tableName, columns)), rowCount))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<String> partitionColumns = table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());

        Map<String, HiveBasicStatistics> partitionBasicStatistics = getPartitionsByNames(databaseName, tableName, ImmutableList.copyOf(partitionNames)).stream()
                .collect(toImmutableMap(
                        partition -> makePartName(partitionColumns, partition.getValues()),
                        partition -> getHiveBasicStatistics(partition.getParameters())));
        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));
        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = getPartitionColumnStatistics(
                databaseName,
                tableName,
                partitionNames,
                dataColumns,
                partitionRowCounts);
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            HiveBasicStatistics basicStatistics = partitionBasicStatistics.getOrDefault(partitionName, createEmptyStatistics());
            Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStatistics.getOrDefault(partitionName, ImmutableMap.of());
            result.put(partitionName, new PartitionStatistics(basicStatistics, columnStatistics));
        }

        return result.build();
    }

    @Override
    public Optional<List<FieldSchema>> getFields(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(MetaException.class, UnknownTableException.class, UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getFields", stats.getGetFields().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    Optional.of(ImmutableList.copyOf(stat.getGetFields().wrapCall(() -> client.getFields(databaseName, tableName)))))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            List<String> columnNames,
            Map<String, OptionalLong> partitionRowCounts)
    {
        return getMetastorePartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames).entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
    }

    private Map<String, List<ColumnStatisticsObj>> getMetastorePartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, List<String> columnNames)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionColumnStatistics", stats.getGetPartitionColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getGetPartitionColumnStatistics().wrapCall(() -> client.getPartitionColumnStatistics(databaseName, tableName, ImmutableList.copyOf(partitionNames), columnNames)))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Map<String, HiveColumnStatistics> groupStatisticsByColumn(List<ColumnStatisticsObj> statistics, OptionalLong rowCount)
    {
        return statistics.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, rowCount)));
    }

    @Override
    public synchronized void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table originalTable = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        Table modifiedTable = originalTable.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        alterTable(databaseName, tableName, modifiedTable);

        com.facebook.presto.hive.metastore.Table table = fromMetastoreApiTable(modifiedTable);
        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), table.getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setTableColumnStatistics(databaseName, tableName, metastoreColumnStatistics);
        }
        Set<String> removedColumnStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedColumnStatistics.forEach(column -> deleteTableColumnStatistics(databaseName, tableName, column));
    }

    private void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setTableColumnStatistics", stats.getUpdateTableColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getUpdateTableColumnStatistics().wrapCall(() -> {
                                        client.setTableColumnStatistics(databaseName, tableName, statistics);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deleteTableColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getUpdatePartitionColumnStatistics().wrapCall(() -> {
                                        client.deleteTableColumnStatistics(databaseName, tableName, columnName);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public synchronized void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = requireNonNull(
                getPartitionStatistics(databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName), "getPartitionStatistics() returned null");
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        List<Partition> partitions = getPartitionsByNames(databaseName, tableName, ImmutableList.of(partitionName));
        if (partitions.size() != 1) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Metastore returned multiple partitions for name: " + partitionName);
        }

        Partition originalPartition = getOnlyElement(partitions);
        Partition modifiedPartition = originalPartition.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedPartition.setParameters(updateStatisticsParameters(modifiedPartition.getParameters(), basicStatistics));
        alterPartitionWithoutStatistics(databaseName, tableName, modifiedPartition);

        Map<String, HiveType> columns = modifiedPartition.getSd().getCols().stream()
                .collect(toImmutableMap(FieldSchema::getName, schema -> HiveType.valueOf(schema.getType())));
        setPartitionColumnStatistics(databaseName, tableName, partitionName, columns, updatedStatistics.getColumnStatistics(), basicStatistics.getRowCount());

        Set<String> removedStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedStatistics.forEach(column -> deletePartitionColumnStatistics(databaseName, tableName, partitionName, column));
    }

    private void setPartitionColumnStatistics(
            String databaseName,
            String tableName,
            String partitionName,
            Map<String, HiveType> columns,
            Map<String, HiveColumnStatistics> columnStatistics,
            OptionalLong rowCount)
    {
        List<ColumnStatisticsObj> metastoreColumnStatistics = columnStatistics.entrySet().stream()
                .filter(entry -> columns.containsKey(entry.getKey()))
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), columns.get(entry.getKey()), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setPartitionColumnStatistics(databaseName, tableName, partitionName, metastoreColumnStatistics);
        }
    }

    private void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setPartitionColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getUpdatePartitionColumnStatistics().wrapCall(() -> {
                                        client.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deletePartitionColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getUpdatePartitionColumnStatistics().wrapCall(() -> {
                                        client.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createRole", stats.getCreateRole().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getCreateRole().wrapCall(() -> {
                                        client.createRole(role, grantor);
                                        return null;
                                    }))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropRole", stats.getDropRole().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getDropRole().wrapCall(() -> {
                                        client.dropRole(role);
                                        return null;
                                    }))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<String> listRoles()
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoles", stats.getListRoles().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> ImmutableSet.copyOf(stat.getListRoles().wrapCall(() -> client.getRoleNames())))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                grantRole(
                        role,
                        grantee.getName(), fromPrestoPrincipalType(grantee.getType()),
                        grantor.getName(), fromPrestoPrincipalType(grantor.getType()),
                        withAdminOption);
            }
        }
    }

    private void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("grantRole", stats.getGrantRole().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getGrantRole().wrapCall(() -> {
                                        client.grantRole(role, granteeName, granteeType, grantorName, grantorType, grantOption);
                                        return null;
                                    }))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                revokeRole(
                        role,
                        grantee.getName(), fromPrestoPrincipalType(grantee.getType()),
                        adminOptionFor);
            }
        }
    }

    private void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("revokeRole", stats.getRevokeRole().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getRevokeRole().wrapCall(() -> {
                                        client.revokeRole(role, granteeName, granteeType, grantOption);
                                        return null;
                                    }))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoleGrants", stats.getListRoleGrants().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    fromRolePrincipalGrants(stat.getListRoleGrants().wrapCall(() -> client.listRoleGrants(principal.getName(), fromPrestoPrincipalType(principal.getType())))))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getGetAllViews().wrapCall(() -> {
                                        String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                                        return Optional.of(client.getTableNamesByFilter(databaseName, filter));
                                    }))));
        }
        catch (UnknownDBException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createDatabase", stats.getCreateDatabase().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getAlterDatabase().wrapCall(() -> {
                                        client.createDatabase(database);
                                        return null;
                                    }))));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidOperationException.class)
                    .stopOnIllegalExceptions()
                    .run("dropDatabase", stats.getAlterDatabase().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getAlterDatabase().wrapCall(() -> {
                                        client.dropDatabase(databaseName, false, false);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @FunctionalInterface
    public interface MetastoreCallable<V>
    {
        V call(HiveMetastoreClient client, ThriftHiveMetastoreStats stats)
                throws Exception;
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterDatabase", stats.getAlterDatabase().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getAlterDatabase().wrapCall(() -> {
                                        client.alterDatabase(databaseName, database);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createTable(Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getCreateTable().wrapCall(() -> {
                                        client.createTable(table);
                                        return null;
                                    }))));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getDropTable().wrapCall(() -> {
                                        client.dropTable(databaseName, tableName, deleteData);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table table)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> {
                                Optional<Table> source = getTable(databaseName, tableName);
                                if (!source.isPresent()) {
                                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                                }
                                return stat.getAlterTable().wrapCall(() -> {
                                    client.alterTable(databaseName, tableName, table);
                                    return null;
                                });
                            })));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    Optional.of(stat.getGetPartitionsByNames().wrapCall(() -> client.getPartitionNames(databaseName, tableName))))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> Optional.of(stat.getGetPartitionNamesPs().wrapCall(() -> client.getPartitionNamesFiltered(databaseName, tableName, parts))))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<String> getPartitionNamesByFilter(String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        List<String> parts = convertPredicateToParts(partitionPredicates);
        return getPartitionNamesByParts(databaseName, tableName, parts).orElse(ImmutableList.of());
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        List<Partition> partitions = partitionsWithStatistics.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPartition)
                .collect(toImmutableList());
        addPartitionsWithoutStatistics(databaseName, tableName, partitions);
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            storePartitionColumnStatistics(databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        }
    }

    private void addPartitionsWithoutStatistics(String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getAddPartitions().wrapCall(() -> {
                                        int partitionsAdded = client.addPartitions(partitions);
                                        if (partitionsAdded != partitions.size()) {
                                            throw new PrestoException(HIVE_METASTORE_ERROR,
                                                    format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                                        }
                                        return null;
                                    }))));
        }
        catch (AlreadyExistsException e) {
            throw new PrestoException(ALREADY_EXISTS, format("One or more partitions already exist for table '%s.%s'", databaseName, tableName), e);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private <V> V wrapMetastoreCallable(MetastoreCallable<V> callable)
            throws Exception
    {
        if (!isHmsImpersonationEnabled) {
            HiveMetastoreClient client = clientProvider.createMetastoreClient(null);
            return callable.call(client, metastoreStats.get(client.getAddress()));
        }
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient(null)) {
            String token = client.getMetastoreClient().get_delegation_token(UserGroupInformation.getCurrentUser().getShortUserName(), UserGroupInformation.getCurrentUser().getShortUserName());
            HiveMetastoreClient realClient = clientProvider.createMetastoreClient(token);
            return callable.call(realClient, metastoreStats.get(realClient.getAddress()));
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getDropPartition().wrapCall(() -> {
                                        client.dropPartition(databaseName, tableName, parts, deleteData);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        alterPartitionWithoutStatistics(databaseName, tableName, toMetastoreApiPartition(partitionWithStatistics));
        storePartitionColumnStatistics(databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        dropExtraColumnStatisticsAfterAlterPartition(databaseName, tableName, partitionWithStatistics);
    }

    private void alterPartitionWithoutStatistics(String databaseName, String tableName, Partition partition)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getAlterPartition().wrapCall(() -> {
                                        client.alterPartition(databaseName, tableName, partition);
                                        return null;
                                    }))));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getValues());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void storePartitionColumnStatistics(String databaseName, String tableName, String partitionName, PartitionWithStatistics partitionWithStatistics)
    {
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        Map<String, HiveColumnStatistics> columnStatistics = statistics.getColumnStatistics();
        if (columnStatistics.isEmpty()) {
            return;
        }
        Map<String, HiveType> columnTypes = partitionWithStatistics.getPartition().getColumns().stream()
                .collect(toImmutableMap(Column::getName, Column::getType));
        setPartitionColumnStatistics(databaseName, tableName, partitionName, columnTypes, columnStatistics, statistics.getBasicStatistics().getRowCount());
    }

    /*
     * After altering a partition metastore preserves all column statistics for that partition.
     *
     * The old statistics are supposed to be replaced by storing the new partition statistics.
     *
     * In case when the new statistics are not present for some columns, or if the table schema has changed
     * if is needed to explicitly remove the statistics from the metastore for that columns.
     */
    private void dropExtraColumnStatisticsAfterAlterPartition(
            String databaseName,
            String tableName,
            PartitionWithStatistics partitionWithStatistics)
    {
        List<String> dataColumns = partitionWithStatistics.getPartition().getColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        Set<String> columnsWithMissingStatistics = new HashSet<>(dataColumns);
        columnsWithMissingStatistics.removeAll(partitionWithStatistics.getStatistics().getColumnStatistics().keySet());

        // In case new partition had the statistics computed for all the columns, the storePartitionColumnStatistics
        // call in the alterPartition will just overwrite the old statistics. There is no need to explicitly remove anything.
        if (columnsWithMissingStatistics.isEmpty()) {
            return;
        }

        // check if statistics for the columnsWithMissingStatistics are actually stored in the metastore
        // when trying to remove any missing statistics the metastore throws NoSuchObjectException
        String partitionName = partitionWithStatistics.getPartitionName();
        List<ColumnStatisticsObj> statisticsToBeRemoved = getMetastorePartitionColumnStatistics(
                databaseName,
                tableName,
                ImmutableSet.of(partitionName),
                ImmutableList.copyOf(columnsWithMissingStatistics))
                .getOrDefault(partitionName, ImmutableList.of());

        for (ColumnStatisticsObj statistics : statisticsToBeRemoved) {
            deletePartitionColumnStatistics(databaseName, tableName, partitionName, statistics.getColName());
        }
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(partitionValues, "partitionValues is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartition", stats.getGetPartition().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    Optional.of(stat.getGetPartition().wrapCall(() -> client.getPartition(databaseName, tableName, partitionValues))))));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) ->
                                    stat.getGetPartitionsByNames().wrapCall(() -> client.getPartitionsByNames(databaseName, tableName, partitionNames)))));
        }
        catch (NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> {
                                Set<HivePrivilegeInfo> existingPrivileges = listTablePrivileges(databaseName, tableName, grantee);
                                Set<PrivilegeGrantInfo> privilegesToGrant = new HashSet<>(requestedPrivileges);
                                Iterator<PrivilegeGrantInfo> iterator = privilegesToGrant.iterator();
                                while (iterator.hasNext()) {
                                    HivePrivilegeInfo requestedPrivilege = getOnlyElement(parsePrivilege(iterator.next(), Optional.empty()));

                                    for (HivePrivilegeInfo existingPrivilege : existingPrivileges) {
                                        if ((requestedPrivilege.isContainedIn(existingPrivilege))) {
                                            iterator.remove();
                                        }
                                        else if (existingPrivilege.isContainedIn(requestedPrivilege)) {
                                            throw new PrestoException(NOT_SUPPORTED, format(
                                                    "Granting %s WITH GRANT OPTION is not supported while %s possesses %s",
                                                    requestedPrivilege.getHivePrivilege().name(),
                                                    grantee,
                                                    requestedPrivilege.getHivePrivilege().name()));
                                        }
                                    }
                                }

                                if (privilegesToGrant.isEmpty()) {
                                    return null;
                                }

                                return stat.getGrantTablePrivileges().wrapCall(() -> client.grantPrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToGrant)));
                            })));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("revokeTablePrivileges", stats.getRevokeTablePrivileges().wrap(() ->
                        wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> {
                            Set<HivePrivilege> existingHivePrivileges = listTablePrivileges(databaseName, tableName, grantee).stream()
                                    .map(HivePrivilegeInfo::getHivePrivilege)
                                    .collect(toSet());

                            Set<PrivilegeGrantInfo> privilegesToRevoke = requestedPrivileges.stream()
                                    .filter(privilegeGrantInfo -> existingHivePrivileges.contains(getOnlyElement(parsePrivilege(privilegeGrantInfo, Optional.empty())).getHivePrivilege()))
                                    .collect(toSet());

                            if (privilegesToRevoke.isEmpty()) {
                                return null;
                            }

                            return stat.getRevokeTablePrivileges().wrapCall(() -> client.revokePrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToRevoke)));
                        })));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getListPrivileges", stats.getListPrivileges().wrap(() ->
                            wrapMetastoreCallable((HiveMetastoreClient client, ThriftHiveMetastoreStats stat) -> {
                                Table table = client.getTable(databaseName, tableName);
                                ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();
                                List<HiveObjectPrivilege> hiveObjectPrivilegeList;
                                // principal can be null when we want to list all privileges for admins
                                if (principal == null) {
                                    hiveObjectPrivilegeList = client.listPrivileges(
                                            null,
                                            null,
                                            new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                                }
                                else {
                                    if (principal.getType() == USER && table.getOwner().equals(principal.getName())) {
                                        privileges.add(new HivePrivilegeInfo(OWNERSHIP, true, principal, principal));
                                    }
                                    hiveObjectPrivilegeList = client.listPrivileges(
                                            principal.getName(),
                                            fromPrestoPrincipalType(principal.getType()),
                                            new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                                }
                                for (HiveObjectPrivilege hiveObjectPrivilege : hiveObjectPrivilegeList) {
                                    PrestoPrincipal grantee = new PrestoPrincipal(fromMetastoreApiPrincipalType(hiveObjectPrivilege.getPrincipalType()), hiveObjectPrivilege.getPrincipalName());
                                    privileges.addAll(parsePrivilege(hiveObjectPrivilege.getGrantInfo(), Optional.of(grantee)));
                                }
                                return privileges.build();
                            })));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private PrivilegeBag buildPrivilegeBag(
            String databaseName,
            String tableName,
            PrestoPrincipal grantee,
            Set<PrivilegeGrantInfo> privilegeGrantInfos)
    {
        ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
        for (PrivilegeGrantInfo privilegeGrantInfo : privilegeGrantInfos) {
            privilegeBagBuilder.add(
                    new HiveObjectPrivilege(
                            new HiveObjectRef(TABLE, databaseName, tableName, null, null),
                            grantee.getName(),
                            fromPrestoPrincipalType(grantee.getType()),
                            privilegeGrantInfo));
        }
        return new PrivilegeBag(privilegeBagBuilder.build());
    }

    private boolean containsAllPrivilege(Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        return requestedPrivileges.stream()
                .anyMatch(privilege -> privilege.getPrivilege().equalsIgnoreCase("all"));
    }

    private RetryDriver retry()
    {
        return RetryDriver.retry()
                .exceptionMapper(exceptionMapper)
                .stopOn(PrestoException.class);
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }
}
