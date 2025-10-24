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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig.HiveMetastoreAuthenticationType;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.RetryDriver;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.TableConstraintAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableConstraintNotFoundException;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreOperationResult.EMPTY_RESULT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.DEFAULT_METASTORE_USER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.deleteDirectoryRecursively;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isManagedTable;
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
import static com.facebook.presto.spi.constraints.TableConstraintsHolder.validateTableConstraints;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.api.HiveObjectType.TABLE;
import static org.apache.hadoop.hive.metastore.api.LockState.ACQUIRED;
import static org.apache.hadoop.hive.metastore.api.LockState.WAITING;
import static org.apache.hadoop.hive.metastore.api.LockType.EXCLUSIVE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

@ThreadSafe
public class ThriftHiveMetastore
        implements HiveMetastore
{
    private static final HdfsContext hdfsContext = new HdfsContext(new ConnectorIdentity(DEFAULT_METASTORE_USER, Optional.empty(), Optional.empty()));

    private final ThriftHiveMetastoreStats stats;
    private final HiveCluster clientProvider;
    private final Function<Exception, Exception> exceptionMapper;
    private final HdfsEnvironment hdfsEnvironment;
    private final boolean impersonationEnabled;
    private final boolean isMetastoreAuthenticationEnabled;
    private final boolean deleteFilesOnTableDrop;

    private volatile boolean metastoreKnownToSupportTableParamEqualsPredicate;
    private volatile boolean metastoreKnownToSupportTableParamLikePredicate;

    @Inject
    public ThriftHiveMetastore(HiveCluster hiveCluster, MetastoreClientConfig config, HdfsEnvironment hdfsEnvironment)
    {
        this(
                hiveCluster,
                new ThriftHiveMetastoreStats(),
                identity(),
                hdfsEnvironment,
                requireNonNull(config, "config is null").isMetastoreImpersonationEnabled(),
                requireNonNull(config, "config is null").isDeleteFilesOnTableDrop(),
                requireNonNull(config, "config is null").getHiveMetastoreAuthenticationType() != HiveMetastoreAuthenticationType.NONE);
    }

    public ThriftHiveMetastore(
            HiveCluster hiveCluster,
            ThriftHiveMetastoreStats stats,
            Function<Exception, Exception> exceptionMapper,
            HdfsEnvironment hdfsEnvironment,
            boolean impersonationEnabled,
            boolean deleteFilesOnTableDrop,
            boolean isMetastoreAuthenticationEnabled)
    {
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.exceptionMapper = requireNonNull(exceptionMapper, "exceptionMapper is null");
        this.impersonationEnabled = impersonationEnabled;
        this.deleteFilesOnTableDrop = deleteFilesOnTableDrop;
        this.isMetastoreAuthenticationEnabled = isMetastoreAuthenticationEnabled;
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

    public Optional<PrimaryKeyConstraint<String>> getPrimaryKey(MetastoreContext metastoreContext, String dbName, String tableName)
    {
        try {
            Optional<PrimaryKeysResponse> pkResponse = retry()
                    .stopOnIllegalExceptions()
                    .run("getPrimaryKey", stats.getGetPrimaryKey().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> client.getPrimaryKey(dbName, tableName))));

            if (!pkResponse.isPresent() || pkResponse.get().getPrimaryKeys().size() == 0) {
                return Optional.empty();
            }

            List<SQLPrimaryKey> pkCols = pkResponse.get().getPrimaryKeys();
            boolean isEnabled = pkCols.get(0).isEnable_cstr();
            boolean isRely = pkCols.get(0).isRely_cstr();
            boolean isEnforced = pkCols.get(0).isValidate_cstr();
            String pkName = pkCols.get(0).getPk_name();
            LinkedHashSet<String> keyCols = pkCols.stream().map(SQLPrimaryKey::getColumn_name).collect(toCollection(LinkedHashSet::new));
            return Optional.of(new PrimaryKeyConstraint<>(Optional.of(pkName), keyCols, isEnabled, isRely, isEnforced));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<UniqueConstraint<String>> getUniqueConstraints(MetastoreContext metastoreContext, String dbName, String tableName)
    {
        try {
            Optional<UniqueConstraintsResponse> uniqueConstraintsResponse = retry()
                    .stopOnIllegalExceptions()
                    .run("getUniqueConstraints", stats.getGetUniqueConstraints().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> client.getUniqueConstraints("hive", dbName, tableName))));

            if (!uniqueConstraintsResponse.isPresent() || uniqueConstraintsResponse.get().getUniqueConstraints().size() == 0) {
                return ImmutableList.of();
            }

            List<SQLUniqueConstraint> uniqueConstraints = uniqueConstraintsResponse.get().getUniqueConstraints();
            //bucket the unique constraint columns by constraint name
            Map<String, List<SQLUniqueConstraint>> bucketedConstraints = uniqueConstraints.stream().collect(Collectors.groupingBy(SQLUniqueConstraint::getUk_name));

            //create a unique table constraint per bucket
            ImmutableList<UniqueConstraint<String>> result = bucketedConstraints.entrySet().stream().map(e -> {
                String constraintName = e.getKey();
                LinkedHashSet<String> columnNames = e.getValue().stream().map(SQLUniqueConstraint::getColumn_name).collect(toCollection(LinkedHashSet::new));
                boolean isEnabled = e.getValue().get(0).isEnable_cstr();
                boolean isRely = e.getValue().get(0).isRely_cstr();
                boolean isEnforced = e.getValue().get(0).isValidate_cstr();
                return new UniqueConstraint<>(Optional.of(constraintName), columnNames, isEnabled, isRely, isEnforced);
            }).collect(toImmutableList());

            return result;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<NotNullConstraint<String>> getNotNullConstraints(MetastoreContext metastoreContext, String dbName, String tableName)
    {
        try {
            Optional<NotNullConstraintsResponse> notNullConstraintsResponse = retry()
                    .stopOnIllegalExceptions()
                    .run("getNotNullConstraints", stats.getGetNotNullConstraints().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> client.getNotNullConstraints("hive", dbName, tableName))));

            if (!notNullConstraintsResponse.isPresent() || notNullConstraintsResponse.get().getNotNullConstraints().size() == 0) {
                return ImmutableList.of();
            }

            ImmutableList<NotNullConstraint<String>> result = notNullConstraintsResponse.get().getNotNullConstraints().stream()
                    .map(constraint -> new NotNullConstraint<>(constraint.getColumn_name()))
                    .collect(toImmutableList());

            return result;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<String> getDatabases(MetastoreContext context, String pattern)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getDatabases", stats.getGetDatabases().wrap(() ->
                            getMetastoreClientThenCall(context, client -> client.getDatabases(pattern))));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
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
        HiveTableHandle hiveTableHandle = new HiveTableHandle(databaseName, tableName);
        return getTable(metastoreContext, hiveTableHandle);
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Table table = client.getTable(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName());
                                if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && !isPrestoView(table)) {
                                    throw new HiveViewNotSupportedException(hiveTableHandle.getSchemaTableName());
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
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE) || type.equals(TIMESTAMP)) {
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (isVarcharType(type) || isCharType(type)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of();
        }
        // Throwing here to make sure this method is updated when a new type is added in Hive connector
        throw new IllegalArgumentException("Unsupported type: " + type);
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
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table originalTable = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        Table modifiedTable = originalTable.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        alterTable(metastoreContext, databaseName, tableName, modifiedTable);

        com.facebook.presto.hive.metastore.Table table = fromMetastoreApiTable(modifiedTable, metastoreContext.getColumnConverter());
        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
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
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
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
                .collect(toImmutableMap(FieldSchema::getName, schema -> metastoreContext.getColumnConverter().toColumn(schema).getType()));
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
                    .run("getAllViews", stats.getGetAllViews().wrap(() -> {
                        return Optional.of(getPrestoViews(databaseName));
                    }));
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
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, List<TableConstraint<String>> constraints)
    {
        List<SQLPrimaryKey> primaryKeys = new ArrayList<>();
        List<SQLUniqueConstraint> uniqueConstraints = new ArrayList<>();
        List<SQLNotNullConstraint> notNullConstraints = new ArrayList<>();
        String callableName = "createTable";
        HiveMetastoreApiStats apiStats = stats.getCreateTable();
        Callable callableClient = apiStats.wrap(() ->
                getMetastoreClientThenCall(metastoreContext, client -> {
                    client.createTable(table);
                    return null;
                }));

        if (!constraints.isEmpty()) {
            validateTableConstraints(constraints);
            for (TableConstraint<String> constraint : constraints) {
                int keySeq = 1;
                if (constraint instanceof PrimaryKeyConstraint) {
                    for (String column : constraint.getColumns()) {
                        primaryKeys.add(
                                new SQLPrimaryKey(
                                        table.getDbName(),
                                        table.getTableName(),
                                        column,
                                        keySeq++,
                                        constraint.getName().orElse(null),
                                        constraint.isEnabled(),
                                        constraint.isEnforced(),
                                        constraint.isRely()));
                    }
                }
                else if (constraint instanceof UniqueConstraint) {
                    for (String column : constraint.getColumns()) {
                        uniqueConstraints.add(
                                new SQLUniqueConstraint(
                                        table.getCatName(),
                                        table.getDbName(),
                                        table.getTableName(),
                                        column,
                                        keySeq++,
                                        constraint.getName().orElse(null),
                                        constraint.isEnabled(),
                                        constraint.isEnforced(),
                                        constraint.isRely()));
                    }
                }
                else if (constraint instanceof NotNullConstraint) {
                    notNullConstraints.add(
                            new SQLNotNullConstraint(
                                    table.getCatName(),
                                    table.getDbName(),
                                    table.getTableName(),
                                    constraint.getColumns().stream().findFirst().get(),
                                    constraint.getName().orElse(null),
                                    constraint.isEnabled(),
                                    constraint.isEnforced(),
                                    constraint.isRely()));
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Constraint %s of unknown type is not supported", constraint.getName().orElse("")));
                }
            }

            callableName = "createTableWithConstraints";
            apiStats = stats.getCreateTableWithConstraints();
            callableClient = apiStats.wrap(() ->
                    getMetastoreClientThenCall(metastoreContext, client -> {
                        client.createTableWithConstraints(table, primaryKeys, uniqueConstraints, notNullConstraints);
                        return null;
                    }));
        }

        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run(callableName, callableClient);
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

        return EMPTY_RESULT;
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
                                if (!deleteFilesOnTableDrop) {
                                    client.dropTable(databaseName, tableName, deleteData);
                                }
                                else {
                                    Table table = client.getTable(databaseName, tableName);
                                    client.dropTable(databaseName, tableName, deleteData);
                                    String tableLocation = table.getSd().getLocation();
                                    if (deleteData && isManagedTable(table.getTableType()) && !isNullOrEmpty(tableLocation)) {
                                        deleteDirectoryRecursively(hdfsContext, hdfsEnvironment, new Path(tableLocation), true);
                                    }
                                }
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
    public MetastoreOperationResult alterTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table table)
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
            return EMPTY_RESULT;
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
    public MetastoreOperationResult alterTableWithEnvironmentContext(MetastoreContext metastoreContext, String databaseName, String tableName, Table table, EnvironmentContext environmentContext)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTableWithEnvironmentContext", stats.getAlterTableWithEnvironmentContext().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                Optional<Table> source = getTable(metastoreContext, databaseName, tableName);
                                if (!source.isPresent()) {
                                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                                }
                                client.alterTableWithEnvironmentContext(databaseName, tableName, table, environmentContext);
                                return null;
                            })));
            return EMPTY_RESULT;
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
    public MetastoreOperationResult addPartitions(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            List<PartitionWithStatistics> partitionsWithStatistics)
    {
        List<Partition> partitions = partitionsWithStatistics.stream()
                .map(part -> ThriftMetastoreUtil.toMetastoreApiPartition(part, metastoreContext.getColumnConverter()))
                .collect(toImmutableList());
        addPartitionsWithoutStatistics(metastoreContext, databaseName, tableName, partitions);
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            storePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        }
        return EMPTY_RESULT;
    }

    private List<String> getPrestoViews(String databaseName)
            throws TException
    {
        /*
         * Thrift call `get_table_names_by_filter` may be translated by Metastore to a SQL query against Metastore database.
         * Hive 2.3 on some databases uses CLOB for table parameter value column and some databases disallow `=` predicate over
         * CLOB values. At the same time, they allow `LIKE` predicates over them.
         */
        String filterWithEquals = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
        String filterWithLike = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " LIKE \"true\"";
        if (metastoreKnownToSupportTableParamEqualsPredicate) {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
                return client.getTableNamesByFilter(databaseName, filterWithEquals);
            }
        }
        if (metastoreKnownToSupportTableParamLikePredicate) {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
                return client.getTableNamesByFilter(databaseName, filterWithLike);
            }
        }
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
            List<String> views = client.getTableNamesByFilter(databaseName, filterWithEquals);
            metastoreKnownToSupportTableParamEqualsPredicate = true;
            return views;
        }
        catch (TException | RuntimeException firstException) {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
                List<String> views = client.getTableNamesByFilter(databaseName, filterWithLike);
                metastoreKnownToSupportTableParamLikePredicate = true;
                return views;
            }
            catch (TException | RuntimeException secondException) {
                if (firstException != secondException) {
                    firstException.addSuppressed(secondException);
                }
            }
            throw firstException;
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
        if (isMetastoreAuthenticationEnabled) {
            String token;
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty())) {
                token = client.getDelegationToken(metastoreContext.getUsername(), metastoreContext.getUsername());
            }
            try (HiveMetastoreClient realClient = clientProvider.createMetastoreClient(Optional.of(token))) {
                return callable.call(realClient);
            }
        }
        HiveMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty());
        setMetastoreUserOrClose(client, metastoreContext.getUsername());
        return callable.call(client);
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
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        alterPartitionWithoutStatistics(metastoreContext, databaseName, tableName, toMetastoreApiPartition(partitionWithStatistics, metastoreContext.getColumnConverter()));
        storePartitionColumnStatistics(metastoreContext, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        dropExtraColumnStatisticsAfterAlterPartition(metastoreContext, databaseName, tableName, partitionWithStatistics);

        return EMPTY_RESULT;
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

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = getTable(metastoreContext, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("dropConstraint", stats.getDropConstraint().wrap(() ->
                            getMetastoreClientThenCall(metastoreContext, client -> {
                                client.dropConstraint(databaseName, tableName, constraintName);
                                return null;
                            })));
            return EMPTY_RESULT;
        }
        catch (NoSuchObjectException e) {
            throw new TableConstraintNotFoundException(Optional.of(constraintName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        Optional<org.apache.hadoop.hive.metastore.api.Table> source = getTable(metastoreContext, databaseName, tableName);
        if (!source.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        org.apache.hadoop.hive.metastore.api.Table table = source.get();
        Set<String> constraintColumns = tableConstraint.getColumns();
        int keySequence = 1;
        List<SQLPrimaryKey> primaryKeyConstraint = new ArrayList<>();
        List<SQLUniqueConstraint> uniqueConstraint = new ArrayList<>();
        List<SQLNotNullConstraint> notNullConstraint = new ArrayList<>();
        String callableName;
        HiveMetastoreApiStats apiStats;
        Callable callableClient;

        if (tableConstraint instanceof PrimaryKeyConstraint) {
            for (String column : constraintColumns) {
                SQLPrimaryKey sqlPrimaryKey = new SQLPrimaryKey(table.getDbName(),
                        table.getTableName(),
                        column,
                        keySequence++,
                        tableConstraint.getName().orElse(null),
                        tableConstraint.isEnabled(),
                        tableConstraint.isEnforced(),
                        tableConstraint.isRely());
                sqlPrimaryKey.setCatName(table.getCatName());
                primaryKeyConstraint.add(sqlPrimaryKey);
            }
            callableName = "addPrimaryKeyConstraint";
            apiStats = stats.getAddPrimaryKeyConstraint();
            callableClient = apiStats.wrap(() ->
                    getMetastoreClientThenCall(metastoreContext, client -> {
                        client.addPrimaryKeyConstraint(primaryKeyConstraint);
                        return null;
                    }));
        }
        else if (tableConstraint instanceof UniqueConstraint) {
            for (String column : constraintColumns) {
                uniqueConstraint.add(
                        new SQLUniqueConstraint(table.getCatName(),
                                table.getDbName(),
                                table.getTableName(),
                                column,
                                keySequence++,
                                tableConstraint.getName().orElse(null),
                                tableConstraint.isEnabled(),
                                tableConstraint.isEnforced(),
                                tableConstraint.isRely()));
            }
            callableName = "addUniqueConstraint";
            apiStats = stats.getAddUniqueConstraint();
            callableClient = apiStats.wrap(() ->
                    getMetastoreClientThenCall(metastoreContext, client -> {
                        client.addUniqueConstraint(uniqueConstraint);
                        return null;
                    }));
        }
        else if (tableConstraint instanceof NotNullConstraint) {
            notNullConstraint.add(
                    new SQLNotNullConstraint(table.getCatName(),
                            table.getDbName(),
                            table.getTableName(),
                            tableConstraint.getColumns().stream().findFirst().get(),
                            tableConstraint.getName().orElse(null),
                            true,
                            true,
                            true));
            callableName = "addNotNullConstraint";
            apiStats = stats.getAddNotNullConstraint();
            callableClient = apiStats.wrap(() ->
                    getMetastoreClientThenCall(metastoreContext, client -> {
                        client.addNotNullConstraint(notNullConstraint);
                        return null;
                    }));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "This connector can only handle Unique/Primary Key/Not Null constraints at this time");
        }

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run(callableName, callableClient);
        }
        catch (AlreadyExistsException e) {
            throw new TableConstraintAlreadyExistsException(tableConstraint.getName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }

        return EMPTY_RESULT;
    }

    @Override
    public long lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            final LockComponent lockComponent = new LockComponent(EXCLUSIVE, LockLevel.TABLE, databaseName);
            lockComponent.setTablename(tableName);
            final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
                    metastoreContext.getUsername(),
                    InetAddress.getLocalHost().getHostName());
            LockResponse lockResponse = stats.getLock().wrap(() -> getMetastoreClientThenCall(metastoreContext, client -> client.lock(lockRequest))).call();
            LockState state = lockResponse.getState();
            long lockId = lockResponse.getLockid();
            final AtomicBoolean acquired = new AtomicBoolean(state.equals(ACQUIRED));

            try {
                if (state.equals(WAITING)) {
                    retry()
                            .maxAttempts(Integer.MAX_VALUE - 100)
                            .stopOnIllegalExceptions()
                            .exceptionMapper(e -> {
                                if (e instanceof WaitingForLockException) {
                                    // only retry on waiting for lock exception
                                    return e;
                                }
                                else {
                                    return new IllegalStateException(e.getMessage(), e);
                                }
                            })
                            .run("lock", stats.getLock().wrap(() ->
                                getMetastoreClientThenCall(metastoreContext, client -> {
                                    LockResponse response = client.checkLock(new CheckLockRequest(lockId));
                                    LockState newState = response.getState();
                                    if (newState.equals(WAITING)) {
                                        throw new WaitingForLockException("Waiting for lock.");
                                    }
                                    else if (newState.equals(ACQUIRED)) {
                                        acquired.set(true);
                                    }
                                    else {
                                        throw new RuntimeException(String.format("Failed to acquire lock: %s", newState.name()));
                                    }
                                    return null;
                                })));
                }
            }
            finally {
                if (!acquired.get()) {
                    unlock(metastoreContext, lockId);
                }
            }

            if (!acquired.get()) {
                throw new RuntimeException("Failed to acquire lock");
            }

            return lockId;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void unlock(MetastoreContext metastoreContext, long lockId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("unlock",
                        stats.getUnlock().wrap(() -> getMetastoreClientThenCall(metastoreContext, client -> {
                            client.unlock(new UnlockRequest(lockId));
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

    private static class WaitingForLockException
            extends RuntimeException
    {
        public WaitingForLockException(String message)
        {
            super(message);
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

    private static void setMetastoreUserOrClose(HiveMetastoreClient client, String username)
            throws TException
    {
        try {
            client.setUGI(username);
        }
        catch (Throwable t) {
            // close client and suppress any error from close
            try (Closeable ignored = client) {
                throw t;
            }
            catch (IOException e) {
                // impossible; will be suppressed
            }
        }
    }
}
