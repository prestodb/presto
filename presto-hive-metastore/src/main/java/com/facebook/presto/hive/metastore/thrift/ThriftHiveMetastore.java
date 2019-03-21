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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
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
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

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
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

@ThreadSafe
public class ThriftHiveMetastore
        implements HiveMetastore
{
    private final ThriftHiveMetastoreStats stats;
    private final HiveCluster clientProvider;
    private final Function<Exception, Exception> exceptionMapper;
    private final boolean impersonationEnabled;

    @Inject
    public ThriftHiveMetastore(HiveCluster hiveCluster, MetastoreClientConfig config)
    {
        this(
                hiveCluster,
                new ThriftHiveMetastoreStats(),
                identity(),
                requireNonNull(config, "config is null").isMetastoreImpersonationEnabled());
    }

    public ThriftHiveMetastore(
            HiveCluster hiveCluster,
            ThriftHiveMetastoreStats stats,
            Function<Exception, Exception> exceptionMapper,
            boolean impersonationEnabled)
    {
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.exceptionMapper = requireNonNull(exceptionMapper, "exceptionMapper is null");
        this.impersonationEnabled = impersonationEnabled;
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

    @Override
    public List<String> getAllDatabases(MetastoreContext context)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() ->
                            getMetastoreClientThenCall(context, HiveMetastoreClient::getAllDatabases)));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> Optional.of(client.getDatabase(databaseName)))));
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
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() ->
                getMetastoreClientThenCall(metastoreContext, client -> client.getAllTables(databaseName)));

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            getMetastoreClientThenCall(metastoreContext, client -> client.getDatabase(databaseName));
            return null;
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
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Table table = client.getTable(databaseName, tableName);
                                if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && !isPrestoView(table)) {
                                    throw new HiveViewNotSupportedException(new SchemaTableName(databaseName, tableName));
                                }
                                return Optional.of(table);
                            })));
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
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(table.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = getTableColumnStatistics(metastoreContext, databaseName, tableName, dataColumns, basicStatistics.getRowCount());
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> columns, OptionalLong rowCount)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTableColumnStatistics", stats.getGetTableColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client ->
                                    groupStatisticsByColumn(client.getTableColumnStatistics(databaseName, tableName, columns), rowCount))));
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
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<String> partitionColumns = table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());

        Map<String, HiveBasicStatistics> partitionBasicStatistics = getPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.copyOf(partitionNames)).stream()
                .collect(toImmutableMap(
                        partition -> makePartName(partitionColumns, partition.getValues()),
                        partition -> getHiveBasicStatistics(partition.getParameters())));
        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));
        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = getPartitionColumnStatistics(
                metastoreContext,
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
    public Optional<List<FieldSchema>> getFields(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(MetaException.class, UnknownTableException.class, UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getFields", stats.getGetFields().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client ->
                                    Optional.of(ImmutableList.copyOf(client.getFields(databaseName, tableName))))));
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
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            List<String> columnNames,
            Map<String, OptionalLong> partitionRowCounts)
    {
        return getMetastorePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionNames, columnNames).entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
    }

    private Map<String, List<ColumnStatisticsObj>> getMetastorePartitionColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames, List<String> columnNames)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionColumnStatistics", stats.getGetPartitionColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client ->
                                    client.getPartitionColumnStatistics(databaseName, tableName, ImmutableList.copyOf(partitionNames), columnNames))));
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
    public synchronized void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table originalTable = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        Table modifiedTable = originalTable.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        alterTable(metastoreContext, databaseName, tableName, modifiedTable);

        com.facebook.presto.hive.metastore.Table table = fromMetastoreApiTable(modifiedTable);
        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
                .filter(entry -> table.getColumn(entry.getKey()).get().getType().getTypeInfo().getCategory() == PRIMITIVE)
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), table.getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setTableColumnStatistics(metastoreContext, databaseName, tableName, metastoreColumnStatistics);
        }
        Set<String> removedColumnStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedColumnStatistics.forEach(column -> deleteTableColumnStatistics(metastoreContext, databaseName, tableName, column));
    }

    private void setTableColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setTableColumnStatistics", stats.getUpdateTableColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.setTableColumnStatistics(databaseName, tableName, statistics);
                                return null;
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

    private void deleteTableColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deleteTableColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.deleteTableColumnStatistics(databaseName, tableName, columnName);
                                return null;
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
    public synchronized void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = requireNonNull(
                getPartitionStatistics(metastoreContext, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName), "getPartitionStatistics() returned null");
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        List<Partition> partitions = getPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.of(partitionName));
        if (partitions.size() != 1) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Metastore returned multiple partitions for name: " + partitionName);
        }

        Partition originalPartition = getOnlyElement(partitions);
        Partition modifiedPartition = originalPartition.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedPartition.setParameters(updateStatisticsParameters(modifiedPartition.getParameters(), basicStatistics));
        alterPartitionWithoutStatistics(metastoreContext, databaseName, tableName, modifiedPartition);

        Map<String, HiveType> columns = modifiedPartition.getSd().getCols().stream()
                .collect(toImmutableMap(FieldSchema::getName, schema -> HiveType.valueOf(schema.getType())));
        setPartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionName, columns, updatedStatistics.getColumnStatistics(), basicStatistics.getRowCount());

        Set<String> removedStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedStatistics.forEach(column -> deletePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionName, column));
    }

    private void setPartitionColumnStatistics(
            MetastoreContext metastoreContext,
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
            setPartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionName, metastoreColumnStatistics);
        }
    }

    private void setPartitionColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setPartitionColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics);
                                return null;
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

    private void deletePartitionColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deletePartitionColumnStatistics", stats.getUpdatePartitionColumnStatistics().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
                                return null;
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
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createRole", stats.getCreateRole().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.createRole(role, grantor);
                                return null;
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
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropRole", stats.getDropRole().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.dropRole(role);
                                return null;
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
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoles", stats.getListRoles().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> ImmutableSet.copyOf(client.getRoleNames()))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                grantRole(
                        metastoreContext,
                        role,
                        grantee.getName(), fromPrestoPrincipalType(grantee.getType()),
                        grantor.getName(), fromPrestoPrincipalType(grantor.getType()),
                        withAdminOption);
            }
        }
    }

    private void grantRole(MetastoreContext metastoreContext, String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("grantRole", stats.getGrantRole().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.grantRole(role, granteeName, granteeType, grantorName, grantorType, grantOption);
                                return null;
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
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                revokeRole(
                        metastoreContext,
                        role,
                        grantee.getName(), fromPrestoPrincipalType(grantee.getType()),
                        adminOptionFor);
            }
        }
    }

    private void revokeRole(MetastoreContext metastoreContext, String role, String granteeName, PrincipalType granteeType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("revokeRole", stats.getRevokeRole().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.revokeRole(role, granteeName, granteeType, grantOption);
                                return null;
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
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoleGrants", stats.getListRoleGrants().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client ->
                                    fromRolePrincipalGrants(client.listRoleGrants(principal.getName(), fromPrestoPrincipalType(principal.getType()))))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                                return Optional.of(client.getTableNamesByFilter(databaseName, filter));
                            })));
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
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createDatabase", stats.getCreateDatabase().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.createDatabase(database);
                                return null;
                            })));
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
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidOperationException.class)
                    .stopOnIllegalExceptions()
                    .run("dropDatabase", stats.getAlterDatabase().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.dropDatabase(databaseName, false, false);
                                return null;
                            })));
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
    public void alterDatabase(MetastoreContext metastoreContext, String databaseName, Database database)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterDatabase", stats.getAlterDatabase().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.alterDatabase(databaseName, database);
                                return null;
                            })));
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
    public void createTable(MetastoreContext metastoreContext, Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.createTable(table);
                                return null;
                            })));
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
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.dropTable(databaseName, tableName, deleteData);
                                return null;
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
    public void alterTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table table)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Optional<Table> source = getTable(metastoreContext, databaseName, tableName);
                                if (!source.isPresent()) {
                                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                                }
                                client.alterTable(databaseName, tableName, table);
                                return null;
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
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> Optional.of(client.getPartitionNames(databaseName, tableName)))));
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
    public Optional<List<String>> getPartitionNamesByParts(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> Optional.of(client.getPartitionNamesFiltered(databaseName, tableName, parts)))));
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
    public List<String> getPartitionNamesByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        List<String> parts = convertPredicateToParts(partitionPredicates);
        return getPartitionNamesByParts(metastoreContext, databaseName, tableName, parts).orElse(ImmutableList.of());
    }

    @Override
    public void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        List<Partition> partitions = partitionsWithStatistics.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPartition)
                .collect(toImmutableList());
        addPartitionsWithoutStatistics(metastoreContext, databaseName, tableName, partitions);
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            storePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        }
    }

    private void addPartitionsWithoutStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                int partitionsAdded = client.addPartitions(partitions);
                                if (partitionsAdded != partitions.size()) {
                                    throw new PrestoException(HIVE_METASTORE_ERROR,
                                            format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                                }
                                return null;
                            })));
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

    private <V> V getMetastoreClientThenCall(MetastoreContext metastoreContext, MetastoreCallable<V> callable)
            throws Exception
    {
        if (!impersonationEnabled) {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
                return callable.call(client);
            }
        }
        String token;
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
            token = client.getDelegationToken(metastoreContext.getUsername(), metastoreContext.getUsername());
        }
        try (HiveMetastoreClient realClient = clientProvider.createMetastoreClient(Optional.of(token))) {
            return callable.call(realClient);
        }
    }

    @FunctionalInterface
    public interface MetastoreCallable<V>
    {
        V call(HiveMetastoreClient client)
                throws Exception;
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.dropPartition(databaseName, tableName, parts, deleteData);
                                return null;
                            })));
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
    public void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        alterPartitionWithoutStatistics(metastoreContext, databaseName, tableName, toMetastoreApiPartition(partitionWithStatistics));
        storePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        dropExtraColumnStatisticsAfterAlterPartition(metastoreContext, databaseName, tableName, partitionWithStatistics);
    }

    private void alterPartitionWithoutStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Partition partition)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.alterPartition(databaseName, tableName, partition);
                                return null;
                            })));
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

    private void storePartitionColumnStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, PartitionWithStatistics partitionWithStatistics)
    {
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        Map<String, HiveColumnStatistics> columnStatistics = statistics.getColumnStatistics();
        if (columnStatistics.isEmpty()) {
            return;
        }
        Map<String, HiveType> columnTypes = partitionWithStatistics.getPartition().getColumns().stream()
                .collect(toImmutableMap(Column::getName, Column::getType));
        setPartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionName, columnTypes, columnStatistics, statistics.getBasicStatistics().getRowCount());
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
            MetastoreContext metastoreContext,
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
                metastoreContext,
                databaseName,
                tableName,
                ImmutableSet.of(partitionName),
                ImmutableList.copyOf(columnsWithMissingStatistics))
                .getOrDefault(partitionName, ImmutableList.of());

        for (ColumnStatisticsObj statistics : statisticsToBeRemoved) {
            deletePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionName, statistics.getColName());
        }
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(partitionValues, "partitionValues is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartition", stats.getGetPartition().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> Optional.of(client.getPartition(databaseName, tableName, partitionValues)))));
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
    public List<Partition> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> client.getPartitionsByNames(databaseName, tableName, partitionNames))));
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
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Set<HivePrivilegeInfo> existingPrivileges = listTablePrivileges(metastoreContext, databaseName, tableName, grantee);
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

                                return client.grantPrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToGrant));
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
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("revokeTablePrivileges", stats.getRevokeTablePrivileges().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Set<HivePrivilege> existingHivePrivileges = listTablePrivileges(metastoreContext, databaseName, tableName, grantee).stream()
                                        .map(HivePrivilegeInfo::getHivePrivilege)
                                        .collect(toSet());

                                Set<PrivilegeGrantInfo> privilegesToRevoke = requestedPrivileges.stream()
                                        .filter(privilegeGrantInfo -> existingHivePrivileges.contains(getOnlyElement(parsePrivilege(privilegeGrantInfo, Optional.empty())).getHivePrivilege()))
                                        .collect(toSet());

                                if (privilegesToRevoke.isEmpty()) {
                                    return null;
                                }

                                return client.revokePrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToRevoke));
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
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getListPrivileges", stats.getListPrivileges().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
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
