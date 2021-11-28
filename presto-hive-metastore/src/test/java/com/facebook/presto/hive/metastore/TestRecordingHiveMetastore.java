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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import com.facebook.presto.hive.metastore.SortingColumn.Order;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.PrestoTableType.OTHER;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_METASTORE_CONTEXT;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestRecordingHiveMetastore
{
    private static final Database DATABASE = new Database(
            "database",
            Optional.of("location"),
            "owner",
            USER,
            Optional.of("comment"),
            ImmutableMap.of("param", "value"));
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            "location",
            Optional.of(new HiveBucketProperty(
                    ImmutableList.of("column"),
                    10,
                    ImmutableList.of(new SortingColumn("column", Order.ASCENDING)),
                    HIVE_COMPATIBLE,
                    Optional.empty())),
            true,
            ImmutableMap.of("param", "value2"),
            ImmutableMap.of());
    private static final Table TABLE = new Table(
            "database",
            "table",
            "owner",
            OTHER,
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value3"),
            Optional.of("original_text"),
            Optional.of("expanded_text"));
    private static final Partition PARTITION = new Partition(
            "database",
            "table",
            ImmutableList.of("value"),
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value4"),
            Optional.empty(),
            false,
            true,
            0);
    private static final PartitionStatistics PARTITION_STATISTICS = new PartitionStatistics(
            new HiveBasicStatistics(10, 11, 10000, 10001),
            ImmutableMap.of("column", new HiveColumnStatistics(
                    Optional.of(new IntegerStatistics(
                            OptionalLong.of(-100),
                            OptionalLong.of(102))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(1234),
                    OptionalLong.of(1235),
                    OptionalLong.of(1),
                    OptionalLong.of(8))));
    private static final HivePrivilegeInfo PRIVILEGE_INFO = new HivePrivilegeInfo(HivePrivilege.SELECT, true, new PrestoPrincipal(USER, "grantor"), new PrestoPrincipal(USER, "grantee"));
    private static final RoleGrant ROLE_GRANT = new RoleGrant(new PrestoPrincipal(USER, "grantee"), "role", true);

    @Test
    public void testRecordingHiveMetastore()
            throws IOException
    {
        MetastoreClientConfig recordingHiveClientConfig = new MetastoreClientConfig()
                .setRecordingPath(File.createTempFile("recording_test", "json").getAbsolutePath())
                .setRecordingDuration(new Duration(10, TimeUnit.MINUTES));

        RecordingHiveMetastore recordingHiveMetastore = new RecordingHiveMetastore(new TestingHiveMetastore(), recordingHiveClientConfig);
        validateMetadata(recordingHiveMetastore);
        recordingHiveMetastore.dropDatabase(TEST_METASTORE_CONTEXT, "other_database");
        recordingHiveMetastore.writeRecording();

        MetastoreClientConfig replayingHiveClientConfig = recordingHiveClientConfig
                .setReplay(true);

        recordingHiveMetastore = new RecordingHiveMetastore(new UnimplementedHiveMetastore(), replayingHiveClientConfig);
        recordingHiveMetastore.loadRecording();
        validateMetadata(recordingHiveMetastore);
    }

    private void validateMetadata(ExtendedHiveMetastore hiveMetastore)
    {
        assertEquals(hiveMetastore.getDatabase(TEST_METASTORE_CONTEXT, "database"), Optional.of(DATABASE));
        assertEquals(hiveMetastore.getAllDatabases(TEST_METASTORE_CONTEXT), ImmutableList.of("database"));
        assertEquals(hiveMetastore.getTable(TEST_METASTORE_CONTEXT, "database", "table"), Optional.of(TABLE));
        assertEquals(hiveMetastore.getSupportedColumnStatistics(TEST_METASTORE_CONTEXT, createVarcharType(123)), ImmutableSet.of(MIN_VALUE, MAX_VALUE));
        assertEquals(hiveMetastore.getTableStatistics(TEST_METASTORE_CONTEXT, "database", "table"), PARTITION_STATISTICS);
        assertEquals(hiveMetastore.getPartitionStatistics(TEST_METASTORE_CONTEXT, "database", "table", ImmutableSet.of("value")), ImmutableMap.of("value", PARTITION_STATISTICS));
        assertEquals(hiveMetastore.getAllTables(TEST_METASTORE_CONTEXT, "database"), Optional.of(ImmutableList.of("table")));
        assertEquals(hiveMetastore.getAllViews(TEST_METASTORE_CONTEXT, "database"), Optional.empty());
        assertEquals(hiveMetastore.getPartition(TEST_METASTORE_CONTEXT, "database", "table", ImmutableList.of("value")), Optional.of(PARTITION));
        assertEquals(hiveMetastore.getPartitionNames(TEST_METASTORE_CONTEXT, "database", "table"), Optional.of(ImmutableList.of("value")));
        Map<Column, Domain> map = new HashMap<>();
        Column column = new Column("column", HiveType.HIVE_STRING, Optional.empty());
        map.put(column, Domain.singleValue(VARCHAR, utf8Slice("value")));
        assertEquals(hiveMetastore.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, "database", "table", map), ImmutableList.of("value"));
        assertEquals(hiveMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, "database", "table", ImmutableList.of("value")), ImmutableMap.of("value", Optional.of(PARTITION)));
        assertEquals(hiveMetastore.listTablePrivileges(TEST_METASTORE_CONTEXT, "database", "table", new PrestoPrincipal(USER, "user")), ImmutableSet.of(PRIVILEGE_INFO));
        assertEquals(hiveMetastore.listRoles(TEST_METASTORE_CONTEXT), ImmutableSet.of("role"));
        assertEquals(hiveMetastore.listRoleGrants(TEST_METASTORE_CONTEXT, new PrestoPrincipal(USER, "user")), ImmutableSet.of(ROLE_GRANT));
    }

    private static class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(DATABASE);
            }

            return Optional.empty();
        }

        @Override
        public List<String> getAllDatabases(MetastoreContext metastoreContext)
        {
            return ImmutableList.of("database");
        }

        @Override
        public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(TABLE);
            }

            return Optional.empty();
        }

        @Override
        public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
        {
            if (type.equals(createVarcharType(123))) {
                return ImmutableSet.of(MIN_VALUE, MAX_VALUE);
            }

            return ImmutableSet.of();
        }

        @Override
        public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return PARTITION_STATISTICS;
            }

            return new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        @Override
        public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", PARTITION_STATISTICS);
            }

            return ImmutableMap.of();
        }

        @Override
        public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(ImmutableList.of("table"));
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
        {
            return Optional.empty();
        }

        @Override
        public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
        {
            // noop for test purpose
        }

        @Override
        public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionValues.equals(ImmutableList.of("value"))) {
                return Optional.of(PARTITION);
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public List<String> getPartitionNamesByFilter(
                MetastoreContext metastoreContext,
                String databaseName,
                String tableName,
                Map<Column, Domain> partitionPredicates)
        {
            List<String> parts = convertPredicateToParts(partitionPredicates);
            if (databaseName.equals("database") && tableName.equals("table") && parts.equals(ImmutableList.of("value"))) {
                return ImmutableList.of("value");
            }

            return ImmutableList.of();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", Optional.of(PARTITION));
            }

            return ImmutableMap.of();
        }

        @Override
        public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String database, String table, PrestoPrincipal prestoPrincipal)
        {
            if (database.equals("database") && table.equals("table") && prestoPrincipal.getType() == USER && prestoPrincipal.getName().equals("user")) {
                return ImmutableSet.of(PRIVILEGE_INFO);
            }

            return ImmutableSet.of();
        }

        @Override
        public Set<String> listRoles(MetastoreContext metastoreContext)
        {
            return ImmutableSet.of("role");
        }

        @Override
        public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
        {
            return ImmutableSet.of(ROLE_GRANT);
        }
    }
}
