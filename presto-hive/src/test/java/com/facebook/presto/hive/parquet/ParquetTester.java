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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.parquet.memory.AggregatedMemoryContext;
import com.facebook.presto.hive.parquet.reader.ParquetMetadataReader;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import parquet.column.ColumnDescriptor;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.succinctBytes;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static parquet.hadoop.metadata.CompressionCodecName.LZO;
import static parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

public class ParquetTester
{
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("Asia/Katmandu");

    private Set<CompressionCodecName> compressions = ImmutableSet.of();

    private Set<WriterVersion> versions = ImmutableSet.of();

    public static ParquetTester quickParquetTester()
    {
        ParquetTester parquetTester = new ParquetTester();
        parquetTester.compressions = ImmutableSet.of(GZIP);
        parquetTester.versions = ImmutableSet.of(PARQUET_1_0);
        return parquetTester;
    }

    public static ParquetTester fullParquetTester()
    {
        ParquetTester parquetTester = new ParquetTester();
        parquetTester.compressions = ImmutableSet.of(GZIP, UNCOMPRESSED, SNAPPY, LZO);
        parquetTester.versions = ImmutableSet.copyOf(WriterVersion.values());
        return parquetTester;
    }

    public void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues, parameterType);
    }

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> readTransform, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, readTransform), parameterType);
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, writeValues, readValues, type);

        // all nulls
        assertRoundTrip(objectInspector, transform(writeValues, constant(null)), transform(readValues, constant(null)), type);
    }

    private void testRoundTripType(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, writeValues, readValues, type);

        // reverse order
        assertRoundTrip(objectInspector, reverse(writeValues), reverse(readValues), type);

        // forward order with nulls
        assertRoundTrip(objectInspector, insertNullEvery(5, writeValues), insertNullEvery(5, readValues), type);

        // reverse order with nulls
        assertRoundTrip(objectInspector, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)), type);
    }

    public void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        for (WriterVersion version : versions) {
            for (CompressionCodecName compressionCodecName : compressions) {
                try (TempFile tempFile = new TempFile("test", "parquet")) {
                    JobConf jobConf = new JobConf();
                    jobConf.setEnum(ParquetOutputFormat.COMPRESSION, compressionCodecName);
                    jobConf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, true);
                    jobConf.setEnum(ParquetOutputFormat.WRITER_VERSION, version);
                    writeParquetColumn(jobConf,
                                    tempFile.getFile(),
                                    compressionCodecName,
                                    objectInspector,
                                    writeValues.iterator());
                    assertFileContents(jobConf,
                                    tempFile,
                                    readValues,
                                    type);
                }
            }
        }
    }

    private static void assertFileContents(JobConf jobConf,
            TempFile tempFile,
            Iterable<?> expectedValues,
            Type type)
            throws IOException, InterruptedException
    {
        Path path = new Path(tempFile.getFile().toURI());
        FileSystem fileSystem = path.getFileSystem(jobConf);
        ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(fileSystem, path);
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();

        long size = fileSystem.getFileStatus(path).getLen();
        FSDataInputStream inputStream = fileSystem.open(path);
        ParquetDataSource dataSource = new HdfsParquetDataSource(path, size, inputStream);

        ParquetReader parquetReader = new ParquetReader(fileSchema, fileSchema, parquetMetadata.getBlocks(), dataSource, TYPE_MANAGER, new AggregatedMemoryContext());
        assertEquals(parquetReader.getPosition(), 0);

        int rowsProcessed = 0;
        Iterator<?> iterator = expectedValues.iterator();
        for (int batchSize = parquetReader.nextBatch(); batchSize >= 0; batchSize = parquetReader.nextBatch()) {
            ColumnDescriptor columnDescriptor = fileSchema.getColumns().get(0);
            Block block = parquetReader.readPrimitive(columnDescriptor, type);
            for (int i = 0; i < batchSize; i++) {
                assertTrue(iterator.hasNext());
                Object expected = iterator.next();
                Object actual = decodeObject(type, block, i);
                assertEquals(actual, expected);
            }
            rowsProcessed += batchSize;
            assertEquals(parquetReader.getPosition(), rowsProcessed);
        }
        assertFalse(iterator.hasNext());

        assertEquals(parquetReader.getPosition(), rowsProcessed);
        parquetReader.close();
    }

    private static DataSize writeParquetColumn(JobConf jobConf,
            File outputFile,
            CompressionCodecName compressionCodecName,
            ObjectInspector columnObjectInspector,
            Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter = new MapredParquetOutputFormat().getHiveRecordWriter(jobConf,
                                            new Path(outputFile.toURI()),
                                            Text.class,
                                            compressionCodecName != UNCOMPRESSED,
                                            createTableProperties("test", columnObjectInspector.getTypeName()),
                                            () -> { }
                                    );
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        int i = 0;
        while (values.hasNext()) {
            Object value = values.next();
            objectInspector.setStructFieldData(row, fields.get(0), value);

            ParquetHiveSerDe serde = new ParquetHiveSerDe();
            serde.initialize(jobConf, createTableProperties("test", columnObjectInspector.getTypeName()), null);
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
            i++;
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(objectInspector));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        return orderTableProperties;
    }

    static class TempFile
            implements Closeable
    {
        private final File file;

        public TempFile(String prefix, String suffix)
        {
            try {
                file = File.createTempFile(prefix, suffix);
                file.delete();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            file.delete();
        }
    }

    private static <T> Iterable<T> reverse(Iterable<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        };
    }

    private static Object decodeObject(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return type.getObjectValue(SESSION, block, position);
    }
}
