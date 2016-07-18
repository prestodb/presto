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
package com.facebook.presto.orc;

import com.facebook.presto.orc.OrcTester.TempFile;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.IntegerStatistics;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.NullMemoryManager;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcWriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.createCustomOrcRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcRecordWriter;
import static com.facebook.presto.orc.OrcTester.createSettableStructObjectInspector;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.testng.Assert.assertEquals;

public class TestOrcReaderPositions
{
    @Test
    public void testEntireFile()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), OrcPredicate.TRUE, BIGINT);
            assertEquals(reader.getReaderRowCount(), 100);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
            assertEquals(reader.getFilePosition(), reader.getReaderPosition());

            for (int i = 0; i < 5; i++) {
                assertEquals(reader.nextBatch(), 20);
                assertEquals(reader.getReaderPosition(), i * 20L);
                assertEquals(reader.getFilePosition(), reader.getReaderPosition());
                assertCurrentBatch(reader, i);
            }

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 100);
            assertEquals(reader.getFilePosition(), reader.getReaderPosition());
            reader.close();
        }
    }

    @Test
    public void testStripeSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            // test reading second and fourth stripes
            OrcPredicate predicate = (numberOfRows, statisticsByColumnIndex) -> {
                if (numberOfRows == 100) {
                    return true;
                }
                IntegerStatistics stats = statisticsByColumnIndex.get(0).getIntegerStatistics();
                return ((stats.getMin() == 60) && (stats.getMax() == 117)) ||
                        ((stats.getMin() == 180) && (stats.getMax() == 237));
            };

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), predicate, BIGINT);
            assertEquals(reader.getFileRowCount(), 100);
            assertEquals(reader.getReaderRowCount(), 40);
            assertEquals(reader.getFilePosition(), 0);
            assertEquals(reader.getReaderPosition(), 0);

            // second stripe
            assertEquals(reader.nextBatch(), 20);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFilePosition(), 20);
            assertCurrentBatch(reader, 1);

            // fourth stripe
            assertEquals(reader.nextBatch(), 20);
            assertEquals(reader.getReaderPosition(), 20);
            assertEquals(reader.getFilePosition(), 60);
            assertCurrentBatch(reader, 3);

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 40);
            assertEquals(reader.getFilePosition(), 100);
            reader.close();
        }
    }

    @Test
    public void testRowGroupSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            // create single strip file with multiple row groups
            int rowCount = 142_000;
            createSequentialFile(tempFile.getFile(), rowCount);

            // test reading two row groups from middle of file
            OrcPredicate predicate = (numberOfRows, statisticsByColumnIndex) -> {
                if (numberOfRows == rowCount) {
                    return true;
                }
                IntegerStatistics stats = statisticsByColumnIndex.get(0).getIntegerStatistics();
                return (stats.getMin() == 50_000) || (stats.getMin() == 60_000);
            };

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), predicate, BIGINT);

            assertEquals(reader.getFileRowCount(), rowCount);
            assertEquals(reader.getReaderRowCount(), rowCount);
            assertEquals(reader.getFilePosition(), 0);
            assertEquals(reader.getReaderPosition(), 0);

            long position = 50_000;
            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(BIGINT, 0);
                for (int i = 0; i < batchSize; i++) {
                    assertEquals(BIGINT.getLong(block, i), position + i);
                }

                assertEquals(reader.getFilePosition(), position);
                assertEquals(reader.getReaderPosition(), position);
                position += batchSize;
            }

            assertEquals(position, 70_000);
            assertEquals(reader.getFilePosition(), rowCount);
            assertEquals(reader.getReaderPosition(), rowCount);
            reader.close();
        }
    }

    @Test
    public void testReadUserMetadata()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Map<String, String> metadata = ImmutableMap.of(
                    "a", "ala",
                    "b", "ma",
                    "c", "kota");
            createFileWithOnlyUserMetadata(tempFile.getFile(), metadata);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
            OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
            Footer footer = orcReader.getFooter();
            Map<String, String> readMetadata = Maps.transformValues(footer.getUserMetadata(), Slice::toStringAscii);
            assertEquals(readMetadata, metadata);
        }
    }

    private static void assertCurrentBatch(OrcRecordReader reader, int stripe)
            throws IOException
    {
        Block block = reader.readBlock(BIGINT, 0);
        for (int i = 0; i < 20; i++) {
            assertEquals(BIGINT.getLong(block, i), ((stripe * 20L) + i) * 3);
        }
    }

    // write 5 stripes of 20 values each: (0,3,6,..,57), (60,..,117), .., (..297)
    private static void createMultiStripeFile(File file)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, OrcTester.Compression.NONE, javaLongObjectInspector);

        @SuppressWarnings("deprecation") Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", javaLongObjectInspector);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < 300; i += 3) {
            if ((i > 0) && (i % 60 == 0)) {
                flushWriter(writer);
            }

            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }

    private static void createFileWithOnlyUserMetadata(File file, Map<String, String> metadata)
            throws IOException
    {
        Configuration conf = new Configuration();
        OrcFile.WriterOptions writerOptions = new OrcWriterOptions(conf)
                .memory(new NullMemoryManager(conf))
                .inspector(createSettableStructObjectInspector("test", javaLongObjectInspector))
                .compress(SNAPPY);
        Writer writer = OrcFile.createWriter(new Path(file.toURI()), writerOptions);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            writer.addUserMetadata(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes(UTF_8)));
        }
        writer.close();
    }

    private static void flushWriter(FileSinkOperator.RecordWriter writer)
            throws IOException, ReflectiveOperationException
    {
        Field field = OrcOutputFormat.class.getClassLoader()
                .loadClass(OrcOutputFormat.class.getName() + "$OrcRecordWriter")
                .getDeclaredField("writer");
        field.setAccessible(true);
        ((Writer) field.get(writer)).writeIntermediateFooter();
    }

    private static void createSequentialFile(File file, int count)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, OrcTester.Compression.NONE, javaLongObjectInspector);

        @SuppressWarnings("deprecation") Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", javaLongObjectInspector);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < count; i++) {
            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }
}
