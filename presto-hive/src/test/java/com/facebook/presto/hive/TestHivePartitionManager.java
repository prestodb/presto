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

import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.partitionColumnHandle;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestHivePartitionManager
{
    private static final String EXPECTED_PARTITION_VALUE = "ds=2017-06-28";
    private static final HiveConnectorId TEST_CONNECTOR_ID = new HiveConnectorId("test_connector");

    private static final SemiTransactionalHiveMetastore HIVE_METASTORE = new SemiTransactionalHiveMetastore(
            new TestingHdfsEnvironment(),
            new TestingExtendedHiveMetastore(),
            directExecutor(),
            true);

    private static final HiveTableHandle HIVE_TABLE_HANDLE = new HiveTableHandle(
            "test",
            "test_table");

    private static final HivePartitionManager HIVE_PARTITION_MANAGER = new HivePartitionManager(
            new TestingTypeManager(),
            UTC,
            true,
            100,
            1);

    private static final List<String> PARTITIONS = ImmutableList.of(
            "ds=2017-06-26",
            "ds=2017-06-27",
            EXPECTED_PARTITION_VALUE);

    @Test
    public void testNoPartitionColumnFiltering()
    {
        assertEquals(
                HIVE_PARTITION_MANAGER.getPartitions(
                        HIVE_METASTORE,
                        HIVE_TABLE_HANDLE,
                        alwaysTrue()).getPartitions().size(),
                PARTITIONS.size());
    }

    @Test
    public void testPartitionColumnFiltering()
            throws Exception
    {
        HivePartitionResult result = HIVE_PARTITION_MANAGER.getPartitions(
                HIVE_METASTORE,
                HIVE_TABLE_HANDLE,
                new Constraint(
                        TupleDomain.fromFixedValues(
                                ImmutableMap.of(
                                        partitionColumnHandle(TEST_CONNECTOR_ID.toString()),
                                        NullableValue.of(
                                                createUnboundedVarcharType(),
                                                utf8Slice(EXPECTED_PARTITION_VALUE)))),
                        bindings -> true));

        assertEquals(result.getPartitions().size(), 1);
        assertEquals(result.getPartitions().get(0).getPartitionId(), EXPECTED_PARTITION_VALUE);
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        public TestingHdfsEnvironment()
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationUpdater(new HiveClientConfig())),
                    new HiveClientConfig(),
                    new NoHdfsAuthentication());
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem();
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int buffersize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingExtendedHiveMetastore
            implements ExtendedHiveMetastore
    {
        public TestingExtendedHiveMetastore()
        {
        }

        @Override
        public Optional<List<String>> getPartitionNamesByParts(
                String databaseName,
                String tableName,
                List<String> parts)
        {
            return Optional.of(PARTITIONS);
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartition(String databaseName, String tableName, Partition partition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRoles(String user)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getAllDatabases()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            return Optional.of(
                    table(
                            databaseName,
                            tableName,
                            ImmutableList.of(new Column("ds", HIVE_STRING, Optional.empty()))));
        }

        @Override
        public Optional<Map<String, HiveColumnStatistics>> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Map<String, Map<String, HiveColumnStatistics>>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getAllTables(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getAllViews(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDatabase(Database database)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDatabase(String databaseName, String newDatabaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(Table table, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(String databaseName, String tableName, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropColumn(String databaseName, String tableName, String columnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
        {
            throw new UnsupportedOperationException();
        }

        private static Table table(
                String databaseName,
                String tableName,
                List<Column> partitionColumns)
        {
            Table.Builder tableBuilder = Table.builder();
            tableBuilder.getStorageBuilder()
                    .setStorageFormat(
                            StorageFormat.create(
                                    "com.facebook.hive.orc.OrcSerde",
                                    "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                    "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                    .setLocation("hdfs://VOL1:9000/db_name/table_name")
                    .setSkewed(false)
                    .setBucketProperty(Optional.empty())
                    .setSorted(false);

            return tableBuilder
                    .setDatabaseName(databaseName)
                    .setOwner("owner")
                    .setTableName(tableName)
                    .setTableType(TableType.MANAGED_TABLE.toString())
                    .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                    .setParameters(ImmutableMap.of())
                    .setPartitionColumns(partitionColumns)
                    .build();
        }
    }

    private static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                    return type;
                }
            }
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters));
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(VARCHAR);
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            throw new UnsupportedOperationException();
        }
    }
}
