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

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import com.facebook.presto.hive.metastore.SortingColumn.Order;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestRecordingHiveMetastore
{
    private static final Database DATABASE = new Database(
            "database",
            Optional.of("location"),
            "owner",
            PrincipalType.USER,
            Optional.of("comment"),
            ImmutableMap.of("param", "value"));
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            "location",
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), 10, ImmutableList.of(new SortingColumn("column", Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));
    private static final Table TABLE = new Table(
            "database",
            "table",
            "owner",
            "table_type",
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
            ImmutableMap.of("param", "value4"));
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
    private static final HivePrivilegeInfo PRIVILEGE_INFO = new HivePrivilegeInfo(HivePrivilege.SELECT, true);

    @Test
    public void testRecordingHiveMetastore()
            throws IOException
    {
        HiveClientConfig recordingHiveClientConfig = new HiveClientConfig()
                .setRecordingPath(File.createTempFile("recording_test", "json").getAbsolutePath())
                .setRecordingDuration(new Duration(10, TimeUnit.MINUTES));

        RecordingHiveMetastore recordingHiveMetastore = new RecordingHiveMetastore(new TestingHiveMetastore(), recordingHiveClientConfig);
        validateMetadata(recordingHiveMetastore);
        recordingHiveMetastore.dropDatabase("other_database");
        recordingHiveMetastore.writeRecording();

        HiveClientConfig replayingHiveClientConfig = recordingHiveClientConfig
                .setReplay(true);

        recordingHiveMetastore = new RecordingHiveMetastore(new UnimplementedHiveMetastore(), replayingHiveClientConfig);
        recordingHiveMetastore.loadRecording();
        validateMetadata(recordingHiveMetastore);
    }

    private void validateMetadata(ExtendedHiveMetastore hiveMetastore)
    {
        assertEquals(hiveMetastore.getDatabase("database"), Optional.of(DATABASE));
        assertEquals(hiveMetastore.getAllDatabases(), ImmutableList.of("database"));
        assertEquals(hiveMetastore.getTable("database", "table"), Optional.of(TABLE));
        assertEquals(hiveMetastore.getSupportedColumnStatistics(createVarcharType(123)), ImmutableSet.of(MIN_VALUE, MAX_VALUE));
        assertEquals(hiveMetastore.getTableStatistics("database", "table"), PARTITION_STATISTICS);
        assertEquals(hiveMetastore.getPartitionStatistics("database", "table", ImmutableSet.of("value")), ImmutableMap.of("value", PARTITION_STATISTICS));
        assertEquals(hiveMetastore.getAllTables("database"), Optional.of(ImmutableList.of("table")));
        assertEquals(hiveMetastore.getAllViews("database"), Optional.empty());
        assertEquals(hiveMetastore.getPartition("database", "table", ImmutableList.of("value")), Optional.of(PARTITION));
        assertEquals(hiveMetastore.getPartitionNames("database", "table"), Optional.of(ImmutableList.of("value")));
        assertEquals(hiveMetastore.getPartitionNamesByParts("database", "table", ImmutableList.of("value")), Optional.of(ImmutableList.of("value")));
        assertEquals(hiveMetastore.getPartitionsByNames("database", "table", ImmutableList.of("value")), ImmutableMap.of("value", Optional.of(PARTITION)));
        assertEquals(hiveMetastore.getRoles("user"), ImmutableSet.of("role1", "role2"));
        assertEquals(hiveMetastore.getDatabasePrivileges("user", "database"), ImmutableSet.of(PRIVILEGE_INFO));
        assertEquals(hiveMetastore.getTablePrivileges("user", "database", "table"), ImmutableSet.of(PRIVILEGE_INFO));
    }

    private static class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(DATABASE);
            }

            return Optional.empty();
        }

        @Override
        public List<String> getAllDatabases()
        {
            return ImmutableList.of("database");
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(TABLE);
            }

            return Optional.empty();
        }

        @Override
        public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
        {
            if (type.equals(createVarcharType(123))) {
                return ImmutableSet.of(MIN_VALUE, MAX_VALUE);
            }

            return ImmutableSet.of();
        }

        @Override
        public PartitionStatistics getTableStatistics(String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return PARTITION_STATISTICS;
            }

            return new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        @Override
        public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", PARTITION_STATISTICS);
            }

            return ImmutableMap.of();
        }

        @Override
        public Optional<List<String>> getAllTables(String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(ImmutableList.of("table"));
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getAllViews(String databaseName)
        {
            return Optional.empty();
        }

        @Override
        public void dropDatabase(String databaseName)
        {
            // noop for test purpose
        }

        @Override
        public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionValues.equals(ImmutableList.of("value"))) {
                return Optional.of(PARTITION);
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
        {
            if (databaseName.equals("database") && tableName.equals("table") && parts.equals(ImmutableList.of("value"))) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", Optional.of(PARTITION));
            }

            return ImmutableMap.of();
        }

        @Override
        public Set<String> getRoles(String user)
        {
            return ImmutableSet.of("role1", "role2");
        }

        @Override
        public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
        {
            if (user.equals("user") && databaseName.equals("database")) {
                return ImmutableSet.of(PRIVILEGE_INFO);
            }

            return ImmutableSet.of();
        }

        @Override
        public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
        {
            if (user.equals("user") && databaseName.equals("database") && tableName.equals("table")) {
                return ImmutableSet.of(PRIVILEGE_INFO);
            }

            return ImmutableSet.of();
        }
    }
}
