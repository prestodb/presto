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

import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.NullMemoryManager;
import org.apache.orc.impl.WriterImpl;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.ZLIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOrcBatchPageSourceMemoryTracking
{
    private static final String ORC_RECORD_WRITER = OrcOutputFormat.class.getName() + "$OrcRecordWriter";
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();
    private static final Configuration CONFIGURATION = new Configuration();
    private static final int NUM_ROWS = 50000;
    private static final int STRIPE_ROWS = 20000;
    private static final MetadataManager metadata = createTestMetadataManager();
    private static final ExpressionCompiler EXPRESSION_COMPILER = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));

    private final Random random = new Random();
    private final List<TestColumn> testColumns = ImmutableList.<TestColumn>builder()
            .add(new TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true))
            .add(new TestColumn("p_string", javaStringObjectInspector, () -> Long.toHexString(random.nextLong()), false))
            .build();

    private File tempFile;
    private TestPreparer testPreparer;

    @DataProvider(name = "rowCount")
    public static Object[][] rowCount()
    {
        return new Object[][] {{50_000}, {10_000}, {5_000}};
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempFile = File.createTempFile("presto_test_orc_page_source_memory_tracking", "orc");
        tempFile.delete();
        testPreparer = new TestPreparer(tempFile.getAbsolutePath());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tempFile.delete();
    }

    @Test
    public void testPageSource()
            throws Exception
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        ConnectorPageSource pageSource = testPreparer.newPageSource(stats);

        assertEquals(pageSource.getSystemMemoryUsage(), 0);

        long memoryUsage = -1;
        int totalRows = 0;
        while (totalRows < 20000) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 180000L, 189999L); // Memory usage before lazy-loading the block
            }
            createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
            memoryUsage = pageSource.getSystemMemoryUsage();
            assertBetweenInclusive(memoryUsage, 390000L, 619999L); // Memory usage after lazy-loading the actual block
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < 40000) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 180000L, 189999L); // Memory usage before lazy-loading the block
            }
            createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
            memoryUsage = pageSource.getSystemMemoryUsage();
            assertBetweenInclusive(memoryUsage, 390000L, 619999L); // Memory usage after lazy-loading the actual block
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < NUM_ROWS) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 90000L, 99999L); // Memory usage before lazy-loading the block
            }
            createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
            memoryUsage = pageSource.getSystemMemoryUsage();
            assertBetweenInclusive(memoryUsage, 430000L, 459999L); // Memory usage after lazy-loading the actual block
            totalRows += page.getPositionCount();
        }

        assertFalse(pageSource.isFinished());
        assertNull(pageSource.getNextPage());
        assertTrue(pageSource.isFinished());
        assertEquals(pageSource.getSystemMemoryUsage(), 0);
        pageSource.close();
    }

    @Test(dataProvider = "rowCount")
    public void testMaxReadBytes(int rowCount)
            throws Exception
    {
        int maxReadBytes = 1_000;
        ConnectorSession session = new TestingConnectorSession(getAllSessionProperties(
                new HiveClientConfig(),
                new HiveCommonClientConfig().setOrcMaxReadBlockSize(new DataSize(maxReadBytes, BYTE))));
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();

        // Build a table where every row gets larger, so we can test that the "batchSize" reduces
        int numColumns = 5;
        int step = 250;
        ImmutableList.Builder<TestColumn> columnBuilder = ImmutableList.<TestColumn>builder()
                .add(new TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true));
        GrowingTestColumn[] dataColumns = new GrowingTestColumn[numColumns];
        for (int i = 0; i < numColumns; i++) {
            dataColumns[i] = new GrowingTestColumn("p_string", javaStringObjectInspector, () -> Long.toHexString(random.nextLong()), false, step * (i + 1));
            columnBuilder.add(dataColumns[i]);
        }
        List<TestColumn> testColumns = columnBuilder.build();
        File tempFile = File.createTempFile("presto_test_orc_page_source_max_read_bytes", "orc");
        tempFile.delete();

        TestPreparer testPreparer = new TestPreparer(tempFile.getAbsolutePath(), testColumns, rowCount, rowCount);
        ConnectorPageSource pageSource = testPreparer.newPageSource(stats, session);

        try {
            int positionCount = 0;
            while (true) {
                Page page = pageSource.getNextPage();
                if (pageSource.isFinished()) {
                    break;
                }
                assertNotNull(page);
                page = page.getLoadedPage();
                positionCount += page.getPositionCount();
                // assert upper bound is tight
                // ignore the first MAX_BATCH_SIZE rows given the sizes are set when loading the blocks
                if (positionCount > MAX_BATCH_SIZE) {
                    // either the block is bounded by maxReadBytes or we just load one single large block
                    // an error margin MAX_BATCH_SIZE / step is needed given the block sizes are increasing
                    assertTrue(page.getSizeInBytes() < maxReadBytes * (MAX_BATCH_SIZE / step) || 1 == page.getPositionCount());
                }
            }

            // verify the stats are correctly recorded
            Distribution distribution = stats.getMaxCombinedBytesPerRow().getAllTime();
            assertEquals((int) distribution.getCount(), 1);
            // the block is VariableWidthBlock that contains valueIsNull and offsets arrays as overhead
            assertEquals((int) distribution.getMax(), Arrays.stream(dataColumns).mapToInt(GrowingTestColumn::getMaxSize).sum() + (Integer.BYTES + Byte.BYTES) * numColumns);
            pageSource.close();
        }
        finally {
            tempFile.delete();
        }
    }

    @Test
    public void testTableScanOperator()
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newTableScanOperator(driverContext);

        assertEquals(driverContext.getSystemMemoryUsage(), 0);

        int totalRows = 0;
        while (totalRows < 20000) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 390000L, 619999L);
            totalRows += page.getPositionCount();
        }

        while (totalRows < 40000) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 390000L, 619999L);
            totalRows += page.getPositionCount();
        }

        while (totalRows < NUM_ROWS) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 430000L, 459999L);
            totalRows += page.getPositionCount();
        }

        assertFalse(operator.isFinished());
        assertNull(operator.getOutput());
        assertTrue(operator.isFinished());
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
    }

    @Test
    public void testScanFilterAndProjectOperator()
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newScanFilterAndProjectOperator(driverContext);

        assertEquals(driverContext.getSystemMemoryUsage(), 0);

        int totalRows = 0;
        while (totalRows < NUM_ROWS) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 90_000L, 619999L);
            totalRows += page.getPositionCount();
        }

        // done... in the current implementation finish is not set until output returns a null page
        assertNull(operator.getOutput());
        assertTrue(operator.isFinished());
        assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 0L, 500L);
    }

    private class TestPreparer
    {
        private final FileSplit fileSplit;
        private final Storage storage;
        private final TableHandle table;
        private final List<HiveColumnHandle> columns;
        private final List<Type> types;
        private final List<HivePartitionKey> partitionKeys;
        private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        public TestPreparer(String tempFilePath)
                throws Exception
        {
            this(tempFilePath, testColumns, NUM_ROWS, STRIPE_ROWS);
        }

        public TestPreparer(String tempFilePath, List<TestColumn> testColumns, int numRows, int stripeRows)
                throws Exception
        {
            OrcSerde serde = new OrcSerde();
            storage = new Storage(
                    StorageFormat.create(serde.getClass().getName(), OrcInputFormat.class.getName(), OrcOutputFormat.class.getName()),
                    "location",
                    Optional.empty(),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of());

            partitionKeys = testColumns.stream()
                    .filter(TestColumn::isPartitionKey)
                    .map(input -> new HivePartitionKey(input.getName(), Optional.ofNullable((String) input.getWriteValue())))
                    .collect(toList());

            table = new TableHandle(
                    new ConnectorId("test"),
                    new ConnectorTableHandle() {},
                    new ConnectorTransactionHandle() {},
                    Optional.empty());

            ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            int nextHiveColumnIndex = 0;
            for (int i = 0; i < testColumns.size(); i++) {
                TestColumn testColumn = testColumns.get(i);
                int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;

                ObjectInspector inspector = testColumn.getObjectInspector();
                HiveType hiveType = HiveType.valueOf(inspector.getTypeName());
                Type type = hiveType.getType(FUNCTION_AND_TYPE_MANAGER);

                columnsBuilder.add(new HiveColumnHandle(testColumn.getName(), hiveType, type.getTypeSignature(), columnIndex, testColumn.isPartitionKey() ? PARTITION_KEY : REGULAR, Optional.empty(), Optional.empty()));
                typesBuilder.add(type);
            }
            columns = columnsBuilder.build();
            types = typesBuilder.build();

            fileSplit = createTestFile(tempFilePath, new OrcOutputFormat(), serde, null, testColumns, numRows, stripeRows);
        }

        public ConnectorPageSource newPageSource()
        {
            return newPageSource(new FileFormatDataSourceStats(), SESSION);
        }

        public ConnectorPageSource newPageSource(FileFormatDataSourceStats stats)
        {
            return newPageSource(stats, SESSION);
        }

        public ConnectorPageSource newPageSource(FileFormatDataSourceStats stats, ConnectorSession session)
        {
            HiveFileSplit hiveFileSplit = new HiveFileSplit(
                    fileSplit.getPath().toString(),
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    fileSplit.getLength(),
                    Instant.now().toEpochMilli(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    0);

            OrcBatchPageSourceFactory orcPageSourceFactory = new OrcBatchPageSourceFactory(
                    FUNCTION_AND_TYPE_MANAGER,
                    HDFS_ENVIRONMENT,
                    stats,
                    100,
                    new StorageOrcFileTailSource(),
                    StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()));
            return HivePageSourceProvider.createHivePageSource(
                    ImmutableSet.of(),
                    ImmutableSet.of(orcPageSourceFactory),
                    new Configuration(),
                    session,
                    hiveFileSplit,
                    OptionalInt.empty(),
                    storage,
                    TupleDomain.all(),
                    columns,
                    ImmutableMap.of(),
                    partitionKeys,
                    DateTimeZone.UTC,
                    FUNCTION_AND_TYPE_MANAGER,
                    new SchemaTableName("schema", "table"),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    0,
                    TableToPartitionMapping.empty(),
                    Optional.empty(),
                    false,
                    DEFAULT_HIVE_FILE_CONTEXT,
                    null,
                    false,
                    ROW_EXPRESSION_SERVICE,
                    Optional.empty(),
                    Optional.empty())
                    .get();
        }

        public SourceOperator newTableScanOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            SourceOperatorFactory sourceOperatorFactory = new TableScanOperatorFactory(
                    0,
                    new PlanNodeId("0"),
                    (session, split, table, columnHandles, runtimeStats) -> pageSource,
                    table,
                    columns.stream().map(columnHandle -> (ColumnHandle) columnHandle).collect(toList()));
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(
                    new ScheduledSplit(
                            0,
                            operator.getSourceId(),
                            new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit())));
            return operator;
        }

        public SourceOperator newScanFilterAndProjectOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            for (int i = 0; i < types.size(); i++) {
                projectionsBuilder.add(field(i, types.get(i)));
            }
            Supplier<CursorProcessor> cursorProcessor = EXPRESSION_COMPILER.compileCursorProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projectionsBuilder.build(), "key");
            Supplier<PageProcessor> pageProcessor = EXPRESSION_COMPILER.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projectionsBuilder.build());
            SourceOperatorFactory sourceOperatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("0"),
                    (session, split, table, columnHandles, runtimeStats) -> pageSource,
                    cursorProcessor,
                    pageProcessor,
                    table,
                    columns.stream().map(columnHandle -> (ColumnHandle) columnHandle).collect(toList()),
                    types,
                    Optional.empty(),
                    new DataSize(0, BYTE),
                    0);
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(
                    new ScheduledSplit(
                            0,
                            operator.getSourceId(),
                            new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit())));
            return operator;
        }

        private DriverContext newDriverContext()
        {
            return createTaskContext(executor, scheduledExecutor, testSessionBuilder().build())
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }
    }

    public static FileSplit createTestFile(String filePath,
            HiveOutputFormat<?, ?> outputFormat,
            Serializer serializer,
            String compressionCodec,
            List<TestColumn> testColumns,
            int numRows,
            int stripeRows)
            throws Exception
    {
        // filter out partition keys, which are not written to the file
        testColumns = ImmutableList.copyOf(filter(testColumns, not(TestColumn::isPartitionKey)));

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(transform(testColumns, TestColumn::getName)));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(testColumns, TestColumn::getType)));
        serializer.initialize(CONFIGURATION, tableProperties);

        JobConf jobConf = new JobConf();
        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(CONFIGURATION).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());
        }

        RecordWriter recordWriter = createRecordWriter(new Path(filePath), CONFIGURATION);

        try {
            SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                    ImmutableList.copyOf(transform(testColumns, TestColumn::getName)),
                    ImmutableList.copyOf(transform(testColumns, TestColumn::getObjectInspector)));

            Object row = objectInspector.create();

            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

            for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
                for (int i = 0; i < testColumns.size(); i++) {
                    Object writeValue = testColumns.get(i).getWriteValue();
                    if (writeValue instanceof Slice) {
                        writeValue = ((Slice) writeValue).getBytes();
                    }
                    objectInspector.setStructFieldData(row, fields.get(i), writeValue);
                }

                Writable record = serializer.serialize(row, objectInspector);
                recordWriter.write(record);
                if (rowNumber % stripeRows == stripeRows - 1) {
                    flushStripe(recordWriter);
                }
            }
        }
        finally {
            recordWriter.close(false);
        }

        Path path = new Path(filePath);
        path.getFileSystem(CONFIGURATION).setVerifyChecksum(true);
        File file = new File(filePath);
        return new FileSplit(path, 0, file.length(), new String[0]);
    }

    private static void flushStripe(RecordWriter recordWriter)
    {
        try {
            Field writerField = OrcOutputFormat.class.getClassLoader()
                    .loadClass(ORC_RECORD_WRITER)
                    .getDeclaredField("writer");
            writerField.setAccessible(true);
            Writer writer = (Writer) writerField.get(recordWriter);
            Method flushStripe = WriterImpl.class.getDeclaredMethod("flushStripe");
            flushStripe.setAccessible(true);
            flushStripe.invoke(writer);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static RecordWriter createRecordWriter(Path target, Configuration conf)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader())) {
            WriterOptions options = OrcFile.writerOptions(conf)
                    .memory(new NullMemoryManager())
                    .compress(ZLIB);

            try {
                return WRITER_CONSTRUCTOR.newInstance(target, options);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Constructor<? extends RecordWriter> getOrcWriterConstructor()
    {
        try {
            Constructor<? extends RecordWriter> constructor = OrcOutputFormat.class.getClassLoader()
                    .loadClass(ORC_RECORD_WRITER)
                    .asSubclass(RecordWriter.class)
                    .getDeclaredConstructor(Path.class, WriterOptions.class);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestColumn
    {
        private final String name;
        private final ObjectInspector objectInspector;
        private final Supplier<?> writeValue;
        private final boolean partitionKey;

        public TestColumn(String name, ObjectInspector objectInspector, Supplier<?> writeValue, boolean partitionKey)
        {
            this.name = requireNonNull(name, "name is null");
            this.objectInspector = requireNonNull(objectInspector, "objectInspector is null");
            this.writeValue = writeValue;
            this.partitionKey = partitionKey;
        }

        public String getName()
        {
            return name;
        }

        public String getType()
        {
            return objectInspector.getTypeName();
        }

        public ObjectInspector getObjectInspector()
        {
            return objectInspector;
        }

        public Object getWriteValue()
        {
            return writeValue.get();
        }

        public boolean isPartitionKey()
        {
            return partitionKey;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("TestColumn{");
            sb.append("name='").append(name).append('\'');
            sb.append(", objectInspector=").append(objectInspector);
            sb.append(", partitionKey=").append(partitionKey);
            sb.append('}');
            return sb.toString();
        }
    }

    public static final class GrowingTestColumn
            extends TestColumn
    {
        private final Supplier<String> writeValue;
        private int counter;
        private int step;
        private int maxSize;

        public GrowingTestColumn(String name, ObjectInspector objectInspector, Supplier<String> writeValue, boolean partitionKey, int step)
        {
            super(name, objectInspector, writeValue, partitionKey);
            this.writeValue = writeValue;
            this.counter = step;
            this.step = step;
        }

        @Override
        public Object getWriteValue()
        {
            StringBuilder builder = new StringBuilder();
            String source = writeValue.get();
            for (int i = 0; i < counter / step; i++) {
                builder.append(source);
            }
            counter++;
            if (builder.length() > maxSize) {
                maxSize = builder.length();
            }
            return builder.toString();
        }

        public int getMaxSize()
        {
            return maxSize;
        }
    }
}
