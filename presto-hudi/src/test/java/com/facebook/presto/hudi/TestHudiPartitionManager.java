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

package com.facebook.presto.hudi;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveColumnHandle.MAX_PARTITION_KEY_COLUMN_INDEX;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestHudiPartitionManager
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String USER_NAME = "user";
    private static final String LOCATION = "somewhere/over/the/rainbow";
    private static final Column PARTITION_COLUMN = new Column("ds", HIVE_STRING, Optional.empty(), Optional.empty());
    private static final Column BUCKET_COLUMN = new Column("c1", HIVE_INT, Optional.empty(), Optional.empty());
    private static final Table TABLE = new Table(
            SCHEMA_NAME,
            TABLE_NAME,
            USER_NAME,
            PrestoTableType.MANAGED_TABLE,
            new Storage(fromHiveStorageFormat(PARQUET),
                    LOCATION,
                    Optional.of(new HiveBucketProperty(
                            ImmutableList.of(BUCKET_COLUMN.getName()),
                            100,
                            ImmutableList.of(),
                            HIVE_COMPATIBLE,
                            Optional.empty())),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            ImmutableList.of(BUCKET_COLUMN),
            ImmutableList.of(PARTITION_COLUMN),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty());

    private static final List<String> PARTITIONS = ImmutableList.of("ds=2019-07-23", "ds=2019-08-23");
    private final HudiPartitionManager hudiPartitionManager = new HudiPartitionManager(new TestingTypeManager());
    private final TestingExtendedHiveMetastore metastore = new TestingExtendedHiveMetastore(TABLE, PARTITIONS);

    @Test
    public void testParseValuesAndFilterPartition()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(100),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        TupleDomain<ColumnHandle> constraintSummary = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new HudiColumnHandle(
                                MAX_PARTITION_KEY_COLUMN_INDEX,
                                PARTITION_COLUMN.getName(),
                                PARTITION_COLUMN.getType(),
                                Optional.empty(),
                                HudiColumnHandle.ColumnType.PARTITION_KEY),
                        Domain.singleValue(VARCHAR, utf8Slice("2019-07-23"))));
        List<String> actualPartitions = hudiPartitionManager.getEffectivePartitions(
                session,
                metastore,
                new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                constraintSummary);
        assertEquals(actualPartitions, ImmutableList.of("ds=2019-07-23"));
    }
}
