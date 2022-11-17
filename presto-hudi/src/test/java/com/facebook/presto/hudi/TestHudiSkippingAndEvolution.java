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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Integration tests for reading Delta tables.
 */
public class TestHudiSkippingAndEvolution
        extends AbstractHudiDistributedQueryTestBase
{
    @Test
    public void testPartitionPruneAndFileSkipping()
    {
        Optional<Table> table = metastore.getTable(METASTORE_CONTEXT, HUDI_SCHEMA, HUDI_SKIPPING_TABLE);
        HudiPartitionManager hudiPartitionManager = new HudiPartitionManager(getQueryRunner().getMetadata().getFunctionAndTypeManager());
        boolean hudiMetadataTableEnabled = isHudiMetadataTableEnabled(connectorSession);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(hudiMetadataTableEnabled).build();

        HoodieTableMetaClient metaClient = HoodieTableMetaClient
                .builder()
                .setConf(new Configuration())
                .setBasePath(table.get().getStorage().getLocation())
                .build();
        // test partition prune by mdt
        // create domain
        List<HudiColumnHandle> partitionColumns = HudiMetadata.fromPartitionColumns(table.get().getPartitionColumns());
        // year=2022 and month=11 and day=12
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                partitionColumns.get(0), Domain.create(ValueSet.of(IntegerType.INTEGER, 2022L), false),
                partitionColumns.get(1), Domain.create(ValueSet.of(IntegerType.INTEGER, 11L), false),
                partitionColumns.get(2), Domain.create(ValueSet.of(IntegerType.INTEGER, 12L), false)));

        List<String> parts = hudiPartitionManager.getEffectivePartitions(connectorSession, metastore, metaClient, table.get().getDatabaseName(), table.get().getTableName(), predicate);

        assertEquals(parts.size(), 1);

        // month = 11
        TupleDomain<ColumnHandle> predicate1 = TupleDomain.withColumnDomains(ImmutableMap.of(
                partitionColumns.get(1), Domain.create(ValueSet.of(IntegerType.INTEGER, 11L), false)));

        List<String> parts1 = hudiPartitionManager.getEffectivePartitions(connectorSession, metastore, metaClient, table.get().getDatabaseName(), table.get().getTableName(), predicate1);

        assertEquals(parts1.size(), 2);

        // test file skipping
        List<HudiColumnHandle> dataColumns = HudiMetadata.fromDataColumns(table.get().getDataColumns());
        List<String> partitions = hudiPartitionManager.getEffectivePartitions(connectorSession, metastore, metaClient, table.get().getDatabaseName(), table.get().getTableName(), TupleDomain.all());
        HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
        HudiFileSkippingManager hudiFileSkippingManager = new HudiFileSkippingManager(
                partitions,
                HudiSessionProperties.getHoodieFilesystemViewSpillableDir(connectorSession),
                engineContext,
                metaClient,
                getQueryType(table.get().getStorage().getStorageFormat().getInputFormat()),
                Option.empty());
        // case1: no filter
        assertEquals(hudiFileSkippingManager.listQueryFiles(TupleDomain.all()).entrySet().stream().map(entry -> entry.getValue().size()).reduce(0, Integer::sum), 4);
        // case2: where col0 > 99, should skip all files
        assertEquals(hudiFileSkippingManager
                .listQueryFiles(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumns.get(7), Domain.create(ValueSet.ofRanges(Range.greaterThan(IntegerType.INTEGER, 99L)), false))))
                .entrySet().stream().map(entry -> entry.getValue().size()).reduce(0, Integer::sum), 0);
        // case3: where col0<=99 and col3 > 1001.0002
        assertEquals(hudiFileSkippingManager
                .listQueryFiles(TupleDomain.withColumnDomains(ImmutableMap.of(
                        dataColumns.get(7), Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(IntegerType.INTEGER, 99L)), false),
                        dataColumns.get(10), Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 1002.0002d)), false))))
                .entrySet().stream().map(entry -> entry.getValue().size()).reduce(0, Integer::sum), 2);
    }

    @Test
    public void testSchemaEvolutionWithColumnRenameDeleteAdd()
    {
        String testQuery = format("SELECT col0,col3_rename,col4_new FROM data_column_rename_drop_add where year=2022 and month=11");
        List<String> expRows = new ArrayList<>();
        expRows.add("SELECT 99,cast(1001.0001 as double),null");
        String expResultsQuery = Joiner.on(" UNION ").join(expRows);
        assertQuery(testQuery, expResultsQuery);
    }

    @Test
    public void testSchemaEvolutionWithColumnTypeChanged()
    {
        String testQuery = format("SELECT col0,col1,col2 FROM data_column_type_change where year=2022 and month=11");
        List<String> expRows = new ArrayList<>();
        expRows.add("SELECT cast(99 as long),'1111111',cast(101.01 as double)");
        String expResultsQuery = Joiner.on(" UNION ").join(expRows);
        assertQuery(testQuery, expResultsQuery);
    }

    private HoodieTableQueryType getQueryType(String hudiInputFormat)
    {
        switch (hudiInputFormat) {
            case "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat":
            case "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat":
                // mor rt table
                return HoodieTableQueryType.SNAPSHOT;
            case "org.apache.hudi.hadoop.HoodieParquetInputFormat":
            case "com.uber.hoodie.hadoop.HoodieInputFormat":
                // cow table/ mor ro table
                return HoodieTableQueryType.READ_OPTIMIZED;
            default:
                throw new IllegalArgumentException(String.format("failed to infer query type for current inputFormat: %s", hudiInputFormat));
        }
    }

    // should remove this function, once we bump hudi to 0.13.0.
    // old hudi-presto-bundle has not include lz4 and caffeine jar which is used by schema evolution and data-skipping.
    private void shouldRemoved()
    {
        XXHashFactory.fastestInstance();
        Caffeine.newBuilder();
    }
}
