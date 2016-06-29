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

import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.GenericCursorProcessor;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.NullMemoryManager;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcWriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.ZLIB;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOrcPageSourceMemoryTracking
{
    private static final String ORC_RECORD_WRITER = OrcOutputFormat.class.getName() + "$OrcRecordWriter";
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();
    private static final Configuration CONFIGURATION = new Configuration();
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final int NUM_ROWS = 50000;
    private static final int STRIPE_ROWS = 20000;

    private final Random random = new Random();
    private final List<TestColumn> testColumns = ImmutableList.<TestColumn>builder()
             .add(new TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true))
             .add(new TestColumn("p_string", javaStringObjectInspector, () -> Long.toHexString(random.nextLong()), false))
             .build();

    private File tempFile;
    private TestPreparer testPreparer;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempFile = File.createTempFile("presto_test_orc_page_source_memory_tracking", "orc");
        tempFile.delete();
        testPreparer = new TestPreparer(tempFile.getAbsolutePath());
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        tempFile.delete();
    }

    @Test
    public void testPageSource()
            throws Exception
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        ConnectorPageSource pageSource = testPreparer.newPageSource();

        assertEquals(pageSource.getSystemMemoryUsage(), 0);

        long memoryUsage = -1;
        for (int i = 0; i < 20; i++) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);
            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 180000L, 189999L); // Memory usage before lazy-loading the block
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L); // Memory usage after lazy-loading the actual block
            }
            else {
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
            }
        }

        memoryUsage = -1;
        for (int i = 20; i < 40; i++) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);
            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 180000L, 189999L); // Memory usage before lazy-loading the block
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L); // Memory usage after lazy-loading the actual block
            }
            else {
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
            }
        }

        memoryUsage = -1;
        for (int i = 40; i < 50; i++) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);
            if (memoryUsage == -1) {
                assertBetweenInclusive(pageSource.getSystemMemoryUsage(), 90000L, 99999L); // Memory usage before lazy-loading the block
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 360000L, 369999L); // Memory usage after lazy-loading the actual block
            }
            else {
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getSystemMemoryUsage(), memoryUsage);
            }
        }

        assertFalse(pageSource.isFinished());
        assertNull(pageSource.getNextPage());
        assertTrue(pageSource.isFinished());
        assertEquals(pageSource.getSystemMemoryUsage(), 0);
        pageSource.close();
    }

    @Test
    public void testTableScanOperator()
            throws Exception
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newTableScanOperator(driverContext);

        assertEquals(driverContext.getSystemMemoryUsage(), 0);

        long memoryUsage = -1;
        for (int i = 0; i < 20; i++) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), memoryUsage);
            }
        }

        memoryUsage = -1;
        for (int i = 20; i < 40; i++) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), memoryUsage);
            }
        }

        memoryUsage = -1;
        for (int i = 40; i < 50; i++) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getSystemMemoryUsage();
                assertBetweenInclusive(memoryUsage, 360000L, 369999L);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), memoryUsage);
            }
        }

        assertFalse(operator.isFinished());
        assertNull(operator.getOutput());
        assertTrue(operator.isFinished());
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
    }

    @Test
    public void testScanFilterAndProjectOperator()
            throws Exception
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newScanFilterAndProjectOperator(driverContext);

        assertEquals(driverContext.getSystemMemoryUsage(), 0);

        for (int i = 0; i < 52; i++) {
            assertFalse(operator.isFinished());
            operator.getOutput();
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 550000L, 639999L);
        }

        for (int i = 52; i < 65; i++) {
            assertFalse(operator.isFinished());
            operator.getOutput();
            assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 450000L, 539999L);
        }

        // Page source is over, but data still exist in buffer of ScanFilterProjectOperator
        assertFalse(operator.isFinished());
        assertNull(operator.getOutput());
        assertBetweenInclusive(driverContext.getSystemMemoryUsage(), 100000L, 109999L);
        assertFalse(operator.isFinished());
        Page lastPage = operator.getOutput();
        assertNotNull(lastPage);

        // No data is left
        assertTrue(operator.isFinished());
        // an empty page builder of two variable width block builders is left in ScanFilterAndProjectOperator
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType()));
        for (int i = 0; i < lastPage.getPositionCount(); i++) {
            pageBuilder.declarePosition();
            createUnboundedVarcharType().appendTo(lastPage.getBlock(0), i, pageBuilder.getBlockBuilder(0));
            createUnboundedVarcharType().appendTo(lastPage.getBlock(1), i, pageBuilder.getBlockBuilder(1));
        }
        pageBuilder.reset();
        assertEquals(driverContext.getSystemMemoryUsage(), pageBuilder.getRetainedSizeInBytes());
    }

    private class TestPreparer
    {
        private final FileSplit fileSplit;
        private final Properties schema;
        private final List<HiveColumnHandle> columns;
        private final List<Type> types;
        private final List<HivePartitionKey> partitionKeys;
        private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        public TestPreparer(String tempFilePath)
                throws Exception
        {
            OrcSerde serde = new OrcSerde();
            schema = new Properties();
            schema.setProperty("columns",
                    testColumns.stream()
                            .map(TestColumn::getName)
                            .collect(Collectors.joining(",")));
            schema.setProperty("columns.types",
                    testColumns.stream()
                            .map(TestColumn::getType)
                            .collect(Collectors.joining(",")));
            schema.setProperty(FILE_INPUT_FORMAT, OrcInputFormat.class.getName());
            schema.setProperty(SERIALIZATION_LIB, serde.getClass().getName());

            partitionKeys = testColumns.stream()
                    .filter(TestColumn::isPartitionKey)
                    .map(input -> new HivePartitionKey(input.getName(), HiveType.valueOf(input.getObjectInspector().getTypeName()), (String) input.getWriteValue()))
                    .collect(toList());

            ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            int nextHiveColumnIndex = 0;
            for (int i = 0; i < testColumns.size(); i++) {
                TestColumn testColumn = testColumns.get(i);
                int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;

                ObjectInspector inspector = testColumn.getObjectInspector();
                HiveType hiveType = HiveType.valueOf(inspector.getTypeName());
                Type type = hiveType.getType(TYPE_MANAGER, false);

                columnsBuilder.add(new HiveColumnHandle("client_id", testColumn.getName(), hiveType, type.getTypeSignature(), columnIndex, testColumn.isPartitionKey() ? PARTITION_KEY : REGULAR));
                typesBuilder.add(type);
            }
            columns = columnsBuilder.build();
            types = typesBuilder.build();

            fileSplit = createTestFile(tempFilePath, new OrcOutputFormat(), serde, null, testColumns, NUM_ROWS);
        }

        public ConnectorPageSource newPageSource()
        {
            OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, false, HDFS_ENVIRONMENT);
            return orcPageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    fileSplit.getPath(),
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    schema,
                    columns,
                    partitionKeys,
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();
        }

        public SourceOperator newTableScanOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            SourceOperatorFactory sourceOperatorFactory = new TableScanOperatorFactory(
                    0,
                    new PlanNodeId("0"),
                    (session, split, columnHandles) -> pageSource,
                    types,
                    columns.stream().map(columnHandle -> (ColumnHandle) columnHandle).collect(toList())
            );
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(new Split("test", TestingTransactionHandle.create("test"), TestingSplit.createLocalSplit()));
            return operator;
        }

        public SourceOperator newScanFilterAndProjectOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            ImmutableList.Builder<ProjectionFunction> projectionsBuilder = ImmutableList.builder();
            for (int i = 0; i < types.size(); i++) {
                projectionsBuilder.add(singleColumn(types.get(i), i));
            }
            ImmutableList<ProjectionFunction> projections = projectionsBuilder.build();
            SourceOperatorFactory sourceOperatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("0"),
                    (session, split, columnHandles) -> pageSource,
                    () -> new GenericCursorProcessor(FilterFunctions.TRUE_FUNCTION, projections),
                    () -> new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, projections),
                    columns.stream().map(columnHandle -> (ColumnHandle) columnHandle).collect(toList()),
                    types
            );
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(new Split("test", TestingTransactionHandle.create("test"), TestingSplit.createLocalSplit()));
            return operator;
        }

        private DriverContext newDriverContext()
        {
            return createTaskContext(executor, testSessionBuilder().build())
                    .addPipelineContext(true, true)
                    .addDriverContext();
        }
    }

    public static FileSplit createTestFile(String filePath,
            HiveOutputFormat<?, ?> outputFormat,
            @SuppressWarnings("deprecation") SerDe serDe,
            String compressionCodec,
            List<TestColumn> testColumns,
            int numRows)
            throws Exception
    {
        // filter out partition keys, which are not written to the file
        testColumns = ImmutableList.copyOf(filter(testColumns, not(TestColumn::isPartitionKey)));

        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(transform(testColumns, TestColumn::getName)));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(testColumns, TestColumn::getType)));
        serDe.initialize(CONFIGURATION, tableProperties);

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

                Writable record = serDe.serialize(row, objectInspector);
                recordWriter.write(record);
                if (rowNumber % STRIPE_ROWS == STRIPE_ROWS - 1) {
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
            throw Throwables.propagate(e);
        }
    }

    private static RecordWriter createRecordWriter(Path target, Configuration conf)
            throws IOException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader())) {
            WriterOptions options = new OrcWriterOptions(conf)
                    .memory(new NullMemoryManager(conf))
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
            throw Throwables.propagate(e);
        }
    }

    public static final class TestColumn
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
}
