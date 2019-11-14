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

import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.HiveTableName;
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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

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

    private TestingSemiTransactionalHiveMetastore(HdfsEnvironment hdfsEnvironment, ExtendedHiveMetastore delegate, HiveMetastoreAuthentication authentication, ListeningExecutorService renameExecutor, boolean skipDeletionForAlter, boolean skipTargetCleanupOnRollback)
    {
        super(hdfsEnvironment, delegate, renameExecutor, authentication, skipDeletionForAlter, skipTargetCleanupOnRollback);
    }

    public static TestingSemiTransactionalHiveMetastore create()
    {
        // none of these values matter, as we never use them
        HiveClientConfig config = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        HiveCluster hiveCluster = new TestingHiveCluster(metastoreClientConfig, HOST, PORT);
        HiveMetastoreAuthentication metastoreAuthentication = new NoHiveMetastoreAuthentication();
        ExtendedHiveMetastore delegate = new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster, metastoreClientConfig, new MBeanExporter(new TestingMBeanServer())));
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        ListeningExecutorService renameExecutor = listeningDecorator(executor);

        return new TestingSemiTransactionalHiveMetastore(hdfsEnvironment, delegate, metastoreAuthentication, renameExecutor, false, false);
    }

    public void addTable(String database, String tableName, Table table, List<String> partitions)
    {
        HiveTableName hiveTableName = new HiveTableName(database, tableName);
        tablesMap.put(hiveTableName, table);
        partitionsMap.put(hiveTableName, partitions);
    }

    @Override
    public synchronized List<String> getAllDatabases(ConnectorIdentity identity)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<Database> getDatabase(ConnectorIdentity identity, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(ConnectorIdentity identity, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<Table> getTable(ConnectorIdentity identity, String databaseName, String tableName)
    {
        return Optional.ofNullable(tablesMap.get(new HiveTableName(databaseName, tableName)));
    }

    @Override
    public synchronized Set<ColumnStatisticType> getSupportedColumnStatistics(ConnectorIdentity identity, Type type)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(ConnectorIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(ConnectorIdentity identity, String databaseName, String tableName, Set<String> partitionNames)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized HivePageSinkMetadata generatePageSinkMetadata(ConnectorIdentity identity, SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<List<String>> getAllViews(ConnectorIdentity identity, String databaseName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createDatabase(ConnectorIdentity identity, Database database)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropDatabase(ConnectorIdentity identity, String schemaName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameDatabase(ConnectorIdentity identity, String source, String target)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void setTableStatistics(ConnectorIdentity identity, Table table, PartitionStatistics tableStatistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void setPartitionStatistics(ConnectorIdentity identity, Table table, Map<List<String>, PartitionStatistics> partitionStatisticsMap)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges, Optional<Path> currentPath, boolean ignoreExisting, PartitionStatistics statistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void replaceView(ConnectorIdentity identity, String databaseName, String tableName, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameTable(ConnectorIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void addColumn(ConnectorIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void renameColumn(ConnectorIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropColumn(ConnectorIdentity identity, String databaseName, String tableName, String columnName)
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

    @Override
    public synchronized Optional<List<String>> getPartitionNames(ConnectorIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByFilter(ConnectorIdentity identity, String databaseName, String tableName, Map<Column, Domain> effectivePredicate)
    {
        return Optional.ofNullable(partitionsMap.get(new HiveTableName(databaseName, tableName)));
    }

    @Override
    public synchronized Optional<Partition> getPartition(ConnectorIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(ConnectorIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void addPartition(ConnectorSession session, String databaseName, String tableName, Partition partition, Path currentLocation, PartitionStatistics statistics)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropPartition(ConnectorSession session, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void finishInsertIntoExistingPartition(ConnectorSession session, String databaseName, String tableName, List<String> partitionValues, Path currentLocation, List<String> fileNames, PartitionStatistics statisticsUpdate)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void createRole(ConnectorIdentity identity, String role, String grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void dropRole(ConnectorIdentity identity, String role)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<String> listRoles(ConnectorIdentity identity)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void grantRoles(ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void revokeRoles(ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(ConnectorIdentity identity, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(ConnectorIdentity identity, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void grantTablePrivileges(ConnectorIdentity identity, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void revokeTablePrivileges(ConnectorIdentity identity, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    public synchronized void declareIntentionToWrite(ConnectorSession session, LocationHandle.WriteMode writeMode, Path stagingPathRoot, Optional<Path> tempPathRoot, String filePrefix, SchemaTableName schemaTableName, boolean temporaryTable)
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
