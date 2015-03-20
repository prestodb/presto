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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

import static io.airlift.testing.Assertions.assertInstanceOf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

/**
 * Created by user on 20-03-2015.
 */
public class InsertTestWorker
{
    private ConnectorSession session;

    protected ConnectorColumnHandle dsColumn;
    protected ConnectorColumnHandle dummyColumn;

    protected HdfsEnvironment hdfsEnvironment;

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorPageSourceProvider pageSourceProvider;
    protected ConnectorRecordSinkProvider recordSinkProvider;

    protected SchemaTableName insertTableDestination;
    protected SchemaTableName insertTablePartitionedDestination;

    protected HiveMetastore metastoreClient;

    public InsertTestWorker(ConnectorSession session,
                            ConnectorColumnHandle dsColumn,
                            ConnectorColumnHandle dummyColumn,
                            HdfsEnvironment hdfsEnvironment,
                            ConnectorMetadata metadata,
                            ConnectorSplitManager splitManager,
                            ConnectorPageSourceProvider pageSourceProvider,
                            ConnectorRecordSinkProvider recordSinkProvider,
                            SchemaTableName insertTableDestination,
                            SchemaTableName insertTablePartitionedDestination,
                            HiveMetastore metastoreClient)
    {
        this.session = session;
        this.dsColumn = dsColumn;
        this.dummyColumn = dummyColumn;
        this.hdfsEnvironment = hdfsEnvironment;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.recordSinkProvider = recordSinkProvider;
        this.insertTableDestination = insertTableDestination;
        this.insertTablePartitionedDestination = insertTablePartitionedDestination;
        this.metastoreClient = metastoreClient;

    }

    public void insertIntoTest()
            throws Exception
    {
        List<String> originalSplits = new ArrayList<String>();
        List<ConnectorSplit> insertCleanupSplits = new ArrayList<ConnectorSplit>();
        try {
            //load existing table
            ConnectorTableHandle tableHandle = getTableHandle(insertTableDestination);
            List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());

            //verify that table has pre-defined data existing and nothing more
            String[] originalCol1Data = {"Val1", "Val2"};
            Integer[] originalCol2Data = {1, 2};

            String[] insertedCol1Data = {"Val3", "Val4"};
            Integer[] insertedCol2Data = {3, 4};

            ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
            ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
            ConnectorSplit originalSplit = getOnlyElement(getAllSplits(splitSource));
            originalSplits.clear();
            originalSplits.add(originalSplit.getInfo().toString());

            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(originalSplit, columnHandles)) {
                verifyInsertData(materializeSourceDataStream(session, pageSource, getTypes(columnHandles)), Arrays.asList(originalCol1Data), Arrays.asList(originalCol2Data));
            }

            doInsertInto(insertedCol1Data, insertedCol2Data);

            // confirm old and new data exists
            tableHandle = getTableHandle(insertTableDestination);
            partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
            assertEquals(partitionResult.getPartitions().size(), 1);
            splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

            boolean foundOriginalSplit = false;
            List<String> col1Data;
            List<Integer> col2Data;
            for (ConnectorSplit split : getAllSplits(splitSource)) {
                if (originalSplits.contains(split.getInfo().toString())) {
                    foundOriginalSplit = true;
                    col1Data = Arrays.asList(originalCol1Data);
                    col2Data = Arrays.asList(originalCol2Data);
                }
                else {
                    col1Data = Arrays.asList(insertedCol1Data);
                    col2Data = Arrays.asList(insertedCol2Data);
                    insertCleanupSplits.add(split);
                }

                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(split, columnHandles)) {
                    verifyInsertData(materializeSourceDataStream(session, pageSource, getTypes(columnHandles)), col1Data, col2Data);
                }
            }

            assertTrue(foundOriginalSplit);
        }
        finally {
            // do this to ensure next time UT is run, we start of with fresh table
            insertCleanupData(insertCleanupSplits);
        }
    }

    public void insertIntoPartitionedTableTest()
            throws Exception
    {
        List<ConnectorSplit> insertCleanupSplits = new ArrayList<ConnectorSplit>();
        List<String> originalSplits = new ArrayList<String>();
        List<HivePartition> insertedPartitions = new ArrayList<HivePartition>();
        try {
            //load table
            ConnectorTableHandle tableHandle = getTableHandle(insertTablePartitionedDestination);
            ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
            ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
            originalSplits.clear();
            originalSplits.addAll(ImmutableList.copyOf(transform(getAllSplits(splitSource), new Function<ConnectorSplit, String>()
            {
                @Override
                public String apply(ConnectorSplit val)
                {
                    return val.getInfo().toString();
                }
            })));

            // read existing data in 2 partitions
            Set<ConnectorPartition> partitions;
            partitions = ImmutableSet.<ConnectorPartition>of(
                    new HivePartition(insertTablePartitionedDestination,
                            TupleDomain.<HiveColumnHandle>all(),
                            "ds=2014-03-12/dummy=1",
                            ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                                    .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2014-03-12")))
                                    .put(dummyColumn, new SerializableNativeValue(Long.class, 1L))
                                    .build(),
                            Optional.<HiveBucketing.HiveBucket>empty()),
                    new HivePartition(insertTablePartitionedDestination,
                            TupleDomain.<HiveColumnHandle>all(),
                            "ds=2014-03-12/dummy=2",
                            ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                                    .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2014-03-12")))
                                    .put(dummyColumn, new SerializableNativeValue(Long.class, 2L))
                                    .build(),
                            Optional.<HiveBucketing.HiveBucket>empty()));

            this.assertExpectedPartitions(partitionResult.getPartitions(), partitions);

            Map<String, List<String>> expectedCol1Data = new HashMap<String, List<String>>();
            Map<String, List<Integer>> expectedCol2Data = new HashMap<String, List<Integer>>();

            String[] col1Data = {"P1_Val1", "P1_Val2", "P2_Val1", "P2_Val2"};
            int[] col2Data = {1, 2, 2, 4};

            expectedCol1Data.put("ds=2014-03-12/dummy=1", ImmutableList.of(col1Data[0], col1Data[1]));
            expectedCol1Data.put("ds=2014-03-12/dummy=2", ImmutableList.of(col1Data[2], col1Data[3]));
            expectedCol2Data.put("ds=2014-03-12/dummy=1", ImmutableList.of(col2Data[0], col2Data[1]));
            expectedCol2Data.put("ds=2014-03-12/dummy=2", ImmutableList.of(col2Data[2], col2Data[3]));

            for (ConnectorPartition part : partitions) {
                verifyPartitionData(tableHandle, part, expectedCol1Data, expectedCol2Data, originalSplits, insertCleanupSplits);
            }

            // insert into these 2 partitions, plus 1 new partition
            String[] col1 = {"P1_Val3", "P2_Val3", "P3_Val1", "P3_Val2"};
            int[] col2 = {3, 6, 8, 10};
            String[] col3 = {"2014-03-12", "2014-03-12", "2014-03-13", "2014-03-13"};
            int[] col4 = {1, 2, 3, 3};

            doPartitionedInsertInto(col1, col2, col3, col4);

            // confirm old and new data exists
            tableHandle = getTableHandle(insertTablePartitionedDestination);
            partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
            assertEquals(partitionResult.getPartitions().size(), 3);

            HivePartition insertedPartition = new HivePartition(insertTablePartitionedDestination,
                    TupleDomain.<HiveColumnHandle>all(),
                    "ds=2014-03-13/dummy=3",
                    ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                            .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2014-03-13")))
                            .put(dummyColumn, new SerializableNativeValue(Long.class, 3L))
                            .build(),
                    Optional.<HiveBucketing.HiveBucket>empty());

            insertedPartitions.add(insertedPartition);

            partitions = ImmutableSet.<ConnectorPartition>builder()
                    .addAll(partitions)
                    .add(insertedPartition)
                    .build();

            assertExpectedPartitions(partitionResult.getPartitions(), partitions);

            expectedCol1Data.put("ds=2014-03-13/dummy=3_new", ImmutableList.of(col1[2], col1[3]));
            expectedCol2Data.put("ds=2014-03-13/dummy=3_new", ImmutableList.of(col2[2], col2[3]));
            expectedCol1Data.put("ds=2014-03-12/dummy=1_new", ImmutableList.<String>builder()
                    .add(col1[0])
                    .build());
            expectedCol2Data.put("ds=2014-03-12/dummy=1_new", ImmutableList.<Integer>builder()
                    .add(col2[0])
                    .build());
            expectedCol1Data.put("ds=2014-03-12/dummy=2_new", ImmutableList.<String>builder()
                    .add(col1[1])
                    .build());
            expectedCol2Data.put("ds=2014-03-12/dummy=2_new", ImmutableList.<Integer>builder()
                    .add(col2[1])
                    .build());

            for (ConnectorPartition part : partitions) {
                verifyPartitionData(tableHandle, part, expectedCol1Data, expectedCol2Data, originalSplits, insertCleanupSplits);
            }
        }
        finally {
            insertCleanupPartitioned(insertedPartitions, insertCleanupSplits);
        }
    }

    private void insertCleanupData(List<ConnectorSplit> insertCleanupSplits)
            throws Exception
    {
        for (ConnectorSplit cs : insertCleanupSplits) {
            HiveSplit split = (HiveSplit) cs;
            Path path = new Path(split.getPath());
            if (hdfsEnvironment.getFileSystem(path).exists(path)) {
                hdfsEnvironment.getFileSystem(path).delete(path, true);
            }
        }
    }

    private void verifyInsertData(MaterializedResult result, List<String> col1Data, List<Integer> col2Data)
    {
        assertEquals(col1Data.size(), col2Data.size());
        assertEquals(result.getRowCount(), col1Data.size());

        MaterializedRow row;

        for (int i = 0; i < col1Data.size(); i++) {
            row = result.getMaterializedRows().get(i);
            assertEquals(row.getField(0), col1Data.get(i));
            assertEquals(row.getField(1), col2Data.get(i).longValue());
        }
    }

    private void doInsertInto(String[] insertedCol1Data, Integer[] insertedCol2Data)
            throws Exception
    {
        //begin insert
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("t_string", VARCHAR, 1, false))
                .add(new ColumnMetadata("t_int", BIGINT, 2, false))
                .build();

        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(session, metadata.getTableHandle(session, insertTableDestination));

        //write data
        RecordSink sink = recordSinkProvider.getRecordSink(insertHandle);

        for (int i = 0; i < insertedCol1Data.length; i++) {
            sink.beginRecord(1);
            sink.appendString(insertedCol1Data[i].getBytes(UTF_8));
            sink.appendLong(insertedCol2Data[i]);
            sink.finishRecord();
        }

        Collection<Slice> fragment = sink.commit();

        //commit insert
        metadata.commitInsert(insertHandle, fragment);
    }

    private void insertCleanupPartitioned(List<HivePartition> insertedPartitions, List<ConnectorSplit> insertCleanupSplits)
            throws Exception
    {
        for (HivePartition partition : insertedPartitions) {
            List<String> partitionVals = ImmutableList.copyOf(transform(Arrays.asList(partition.getPartitionId().split("/")), new Function<String, String>() {
                @Override
                public String apply(String val)
                {
                    return val.split("=")[1];
                }
            }));

            metastoreClient.dropPartition(partition.getTableName().getSchemaName(),
                    partition.getTableName().getTableName(),
                    partitionVals,
                    true);
        }
        insertCleanupData(insertCleanupSplits);
    }

    private void verifyPartitionData(ConnectorTableHandle tableHandle, ConnectorPartition partition, Map<String, List<String>> col1Data, Map<String, List<Integer>> col2Data, List<String> originalSplits, List<ConnectorSplit> insertCleanupSplits)
            throws Exception
    {
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());

        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, ImmutableList.<ConnectorPartition>of(partition));
        for (ConnectorSplit split : getAllSplits(splitSource)) {
            String key = partition.getPartitionId();
            if (!originalSplits.contains(split.getInfo().toString())) {
                insertCleanupSplits.add(split);
                key = key + "_new";
            }
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(split, columnHandles)) {
                verifyInsertData(materializeSourceDataStream(session, pageSource, getTypes(columnHandles)), col1Data.get(key), col2Data.get(key));
            }
        }
    }

    private void doPartitionedInsertInto(String[] col1, int[] col2, String[] col3, int[] col4)
            throws Exception
    {
        //begin insert
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("t_string", VARCHAR, 1, false))
                .add(new ColumnMetadata("t_int", BIGINT, 2, false))
                .add(new ColumnMetadata("ds", VARCHAR, 3, true))
                .add(new ColumnMetadata("dummy", BIGINT, 2, true))
                .build();

        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(session, metadata.getTableHandle(session, insertTablePartitionedDestination));

        //write data
        RecordSink sink = recordSinkProvider.getRecordSink(insertHandle);

        for (int i = 0; i < col1.length; i++) {
            sink.beginRecord(1);
            sink.appendString(col1[i].getBytes(UTF_8));
            sink.appendLong(col2[i]);
            sink.appendString(col3[i].getBytes(UTF_8));
            sink.appendLong(col4[i]);
            sink.finishRecord();
        }

        Collection<Slice> fragment = sink.commit();

        // commit insert
        metadata.commitInsert(insertHandle, fragment);

    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(session, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000);
            splits.addAll(batch);
        }
        return splits.build();

    }

    protected void assertExpectedPartitions(List<ConnectorPartition> actualPartitions, Iterable<ConnectorPartition> expectedPartitions)
    {
        Map<String, ConnectorPartition> actualById = uniqueIndex(actualPartitions, ConnectorPartition::getPartitionId);
        for (ConnectorPartition expected : expectedPartitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            ConnectorPartition actual = actualById.get(expectedPartition.getPartitionId());
            assertEquals(actual, expected);
            assertInstanceOf(actual, HivePartition.class);
            HivePartition actualPartition = (HivePartition) actual;

            assertNotNull(actualPartition, "partition " + expectedPartition.getPartitionId());
            assertEquals(actualPartition.getPartitionId(), expectedPartition.getPartitionId());
            assertEquals(actualPartition.getKeys(), expectedPartition.getKeys());
            assertEquals(actualPartition.getTableName(), expectedPartition.getTableName());
            assertEquals(actualPartition.getBucket(), expectedPartition.getBucket());
            assertEquals(actualPartition.getTupleDomain(), expectedPartition.getTupleDomain());
        }
    }
}
