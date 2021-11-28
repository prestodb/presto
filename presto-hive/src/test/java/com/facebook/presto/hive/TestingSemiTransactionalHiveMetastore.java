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

package com.facebook.presto.hive;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.HiveTableName;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestingSemiTransactionalHiveMetastore
        extends SemiTransactionalHiveMetastore
{
    private static final String HOST = "dummy";
    private static final int PORT = 1111;

    private final Map<HiveTableName, Table> tablesMap = new HashMap<>();
    private final Map<HiveTableName, List<String>> partitionsMap = new HashMap<>();

    private List<String> partitionNames;

    private TestingSemiTransactionalHiveMetastore(HdfsEnvironment hdfsEnvironment, ExtendedHiveMetastore delegate, ListeningExecutorService renameExecutor, boolean skipDeletionForAlter, boolean skipTargetCleanupOnRollback, boolean undoMetastoreOperationsEnabled)
    {
        super(hdfsEnvironment, delegate, renameExecutor, skipDeletionForAlter, skipTargetCleanupOnRollback, undoMetastoreOperationsEnabled);
    }

    public static TestingSemiTransactionalHiveMetastore create()
    {
        // none of these values matter, as we never use them
        HiveClientConfig config = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        HiveCluster hiveCluster = new TestingHiveCluster(metastoreClientConfig, HOST, PORT);
        ExtendedHiveMetastore delegate = new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster, metastoreClientConfig), new HivePartitionMutator());
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        ListeningExecutorService renameExecutor = listeningDecorator(executor);

        return new TestingSemiTransactionalHiveMetastore(hdfsEnvironment, delegate, renameExecutor, false, false, true);
    }

    public void addTable(String database, String tableName, Table table, List<String> partitions)
    {
        HiveTableName hiveTableName = new HiveTableName(database, tableName);
        tablesMap.put(hiveTableName, table);
        partitionsMap.put(hiveTableName, partitions);
    }

    @Override
    public synchronized List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return Optional.ofNullable(tablesMap.get(new HiveTableName(databaseName, tableName)));
    }

    @Override
    public synchronized Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized HivePageSinkMetadata generatePageSinkMetadata(MetastoreContext metastoreContext, SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropDatabase(MetastoreContext metastoreContext, String schemaName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameDatabase(MetastoreContext metastoreContext, String source, String target)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void setTableStatistics(MetastoreContext metastoreContext, Table table, PartitionStatistics tableStatistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void setPartitionStatistics(MetastoreContext metastoreContext, Table table, Map<List<String>, PartitionStatistics> partitionStatisticsMap)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges, Optional<Path> currentPath, boolean ignoreExisting, PartitionStatistics statistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropTable(HdfsContext context, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void replaceView(MetastoreContext metastoreContext, String databaseName, String tableName, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void finishInsertIntoExistingTable(ConnectorSession session, String databaseName, String tableName, Path currentLocation, List<String> fileNames, PartitionStatistics statisticsUpdate)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void truncateUnpartitionedTable(ConnectorSession session, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    public synchronized void setPartitionNames(List<String> partitionNames)
    {
        this.partitionNames = partitionNames;
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return Optional.ofNullable(partitionNames);
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> effectivePredicate)
    {
        return Optional.ofNullable(partitionsMap.get(new HiveTableName(databaseName, tableName)));
    }

    @Override
    public synchronized Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void addPartition(ConnectorSession session, String databaseName, String tableName, String tablePath, boolean isNewTable, Partition partition, Path currentLocation, PartitionStatistics statistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropPartition(ConnectorSession session, String databaseName, String tableName, String tablePath, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void finishInsertIntoExistingPartition(ConnectorSession session, String databaseName, String tableName, String tablePath, List<String> partitionValues, Path currentLocation, List<String> fileNames, PartitionStatistics statisticsUpdate)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropRole(MetastoreContext metastoreContext, String role)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<String> listRoles(MetastoreContext metastoreContext)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void declareIntentionToWrite(HdfsContext context, MetastoreContext metastoreContext, LocationHandle.WriteMode writeMode, Path stagingPathRoot, Optional<Path> tempPathRoot, SchemaTableName schemaTableName, boolean temporaryTable)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void commit()
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void rollback()
    {
        throw new UnsupportedOperationException("method not implemented");
    }
}
