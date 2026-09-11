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
package com.facebook.presto.hive.functions.schema;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.functions.schema.ParquetSchemaExtractor.extractFromDataSource;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestParquetSchemaExtractor
{
    private static final TypeManager TYPE_MANAGER = FUNCTION_AND_TYPE_MANAGER;

    private java.nio.file.Path tempDir;

    private static ColumnMetadata col(String name, Type type, boolean nullable)
    {
        return ColumnMetadata.builder().setName(name).setType(type).setNullable(nullable).build();
    }

    @BeforeClass
    public void setUp()
            throws IOException
    {
        tempDir = createTempDirectory("parquet-schema-extractor-test");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (tempDir != null) {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testFlatRequired()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .addField(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name"))
                .addField(Types.required(PrimitiveTypeName.DOUBLE).named("value"))
                .named("root");
        File file = writeEmptyParquet("flat.parquet", schema);

        List<ColumnMetadata> columns = extract(file);
        assertEquals(columns, ImmutableList.of(
                col("id", BIGINT, false),
                col("name", VARCHAR, false),
                col("value", DOUBLE, false)));
    }

    @Test
    public void testOptionals()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("optional_name"))
                .named("root");
        File file = writeEmptyParquet("optionals.parquet", schema);

        List<ColumnMetadata> columns = extract(file);
        assertEquals(columns.size(), 2);
        assertEquals(columns.get(0), col("id", BIGINT, false));
        assertEquals(columns.get(1), col("optional_name", VARCHAR, true));
    }

    @Test
    public void testDecimalsAndTemporal()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optional(PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.decimalType(4, 18)).named("short_dec"))
                .addField(Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                        .as(LogicalTypeAnnotation.decimalType(10, 38)).named("long_dec"))
                .addField(Types.optional(PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.dateType()).named("d"))
                .addField(Types.optional(PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("ts"))
                .named("root");
        File file = writeEmptyParquet("decimals_temporal.parquet", schema);

        List<ColumnMetadata> columns = extract(file);
        assertEquals(columns, ImmutableList.of(
                col("short_dec", DecimalType.createDecimalType(18, 4), true),
                col("long_dec", DecimalType.createDecimalType(38, 10), true),
                col("d", DATE, true),
                col("ts", TIMESTAMP_WITH_TIME_ZONE, true)));
    }

    @Test
    public void testNestedRowArrayMap()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalGroup()
                        .addField(Types.required(PrimitiveTypeName.INT64).named("a"))
                        .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b"))
                        .named("payload"))
                .addField(Types.optionalList()
                        .element(Types.required(PrimitiveTypeName.INT64).named("element"))
                        .named("arr"))
                .addField(Types.optionalList()
                        .element(Types.optionalGroup()
                                .addField(Types.required(PrimitiveTypeName.INT64).named("a"))
                                .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b"))
                                .named("element"))
                        .named("arr_row"))
                .addField(Types.optionalMap()
                        .key(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key"))
                        .value(Types.optional(PrimitiveTypeName.INT64).named("value"))
                        .named("m"))
                .named("root");
        File file = writeEmptyParquet("nested.parquet", schema);

        List<ColumnMetadata> columns = extract(file);
        assertEquals(columns.size(), 4);

        Type expectedPayload = RowType.from(ImmutableList.of(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR)));
        assertEquals(columns.get(0), col("payload", expectedPayload, true));
        assertEquals(columns.get(1), col("arr", new ArrayType(BIGINT), true));
        assertEquals(columns.get(2), col("arr_row", new ArrayType(expectedPayload), true));

        Type expectedMap = TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(VARCHAR.getTypeSignature()),
                        TypeSignatureParameter.of(BIGINT.getTypeSignature())));
        assertEquals(columns.get(3), col("m", expectedMap, true));
    }

    @Test
    public void testEmptyValidFileReturnsSchema()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .named("root");
        File file = writeEmptyParquet("empty.parquet", schema);

        List<ColumnMetadata> columns = extract(file);
        assertEquals(columns, ImmutableList.of(col("id", BIGINT, false)));
    }

    @Test
    public void testCorruptFileThrowsHiveBadData()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .named("root");
        File valid = writeEmptyParquet("corrupt.parquet", schema);
        // Drop the trailing 16 bytes (magic + footer length).
        try (RandomAccessFile raf = new RandomAccessFile(valid, "rw")) {
            raf.setLength(Math.max(0L, valid.length() - 16));
        }

        try {
            extract(valid);
            fail("Expected PrestoException(HIVE_BAD_DATA)");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_BAD_DATA.toErrorCode());
            assertTrue(e.getMessage().contains(valid.getName()), "message should name the file: " + e.getMessage());
        }
    }

    @Test
    public void testNonParquetFileThrowsHiveBadData()
            throws IOException
    {
        File file = new File(tempDir.toFile(), "not_parquet.bin");
        Files.write(file.toPath(), "this is not a parquet file, just plain text".getBytes(UTF_8));

        try {
            extract(file);
            fail("Expected PrestoException(HIVE_BAD_DATA)");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_BAD_DATA.toErrorCode());
            assertTrue(e.getMessage().contains(file.getName()), "message should name the file: " + e.getMessage());
        }
    }

    @Test
    public void testReadsOnlyFooterBytes()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name"))
                .named("root");
        // 1000 rows to ensure there's a real body before the footer.
        File file = writePopulatedParquet("populated.parquet", schema, 1000);

        long fileSize = file.length();
        try (FileParquetDataSource backing = new FileParquetDataSource(file)) {
            RecordingDataSource recording = new RecordingDataSource(backing);
            List<ColumnMetadata> columns = extractFromDataSource(recording, new Path(file.toURI()), fileSize, TYPE_MANAGER);
            assertEquals(columns.size(), 2);

            // Any read outside the trailing footer-size window must be a data-block read.
            long footerWindowStart = Math.max(0L, fileSize - 16 * 1024);
            assertFalse(recording.reads.isEmpty(), "expected at least one read");
            for (long[] read : recording.reads) {
                long position = read[0];
                assertTrue(
                        position >= footerWindowStart,
                        String.format("read at position %s falls outside the footer window [%s, %s)", position, footerWindowStart, fileSize));
            }
        }
    }

    @Test
    public void testExtractEndToEndViaHdfsEnvironment()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                .named("root");
        File file = writeEmptyParquet("e2e.parquet", schema);

        ParquetSchemaExtractor extractor = new ParquetSchemaExtractor(TYPE_MANAGER, new FileFormatDataSourceStats());
        List<ColumnMetadata> columns = extractor.extract(new Path(file.toURI()), HDFS_ENVIRONMENT, SESSION);
        assertEquals(columns, ImmutableList.of(col("id", BIGINT, false)));
    }

    private List<ColumnMetadata> extract(File file)
            throws IOException
    {
        try (FileParquetDataSource source = new FileParquetDataSource(file)) {
            return extractFromDataSource(source, new Path(file.toURI()), file.length(), TYPE_MANAGER);
        }
    }

    private File writeEmptyParquet(String name, MessageType schema)
            throws IOException
    {
        File file = new File(tempDir.toFile(), name);
        if (file.exists() && !file.delete()) {
            throw new IOException("Could not delete pre-existing file " + file);
        }
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        try (ParquetWriter<?> ignored = ExampleParquetWriter.builder(new Path(file.toURI()))
                .withConf(conf)
                .withWriteMode(OVERWRITE)
                .withType(schema)
                .build()) {
            // Close without writing rows: emits an empty file with just the schema footer.
        }
        return file;
    }

    private File writePopulatedParquet(String name, MessageType schema, int rows)
            throws IOException
    {
        File file = new File(tempDir.toFile(), name);
        if (file.exists() && !file.delete()) {
            throw new IOException("Could not delete pre-existing file " + file);
        }
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        try (ParquetWriter<org.apache.parquet.example.data.Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
                .withConf(conf)
                .withWriteMode(OVERWRITE)
                .withType(schema)
                .build()) {
            org.apache.parquet.example.data.simple.SimpleGroupFactory factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
            for (int i = 0; i < rows; i++) {
                org.apache.parquet.example.data.Group group = factory.newGroup()
                        .append("id", (long) i)
                        .append("name", "row-" + i);
                writer.write(group);
            }
        }
        return file;
    }

    private static final class RecordingDataSource
            implements ParquetDataSource
    {
        private final ParquetDataSource delegate;
        private final List<long[]> reads = new ArrayList<>();

        RecordingDataSource(ParquetDataSource delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public ParquetDataSourceId getId()
        {
            return delegate.getId();
        }

        @Override
        public long getReadBytes()
        {
            return delegate.getReadBytes();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public void readFully(long position, byte[] buffer)
        {
            reads.add(new long[] {position, buffer.length});
            delegate.readFully(position, buffer);
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            reads.add(new long[] {position, bufferLength});
            delegate.readFully(position, buffer, bufferOffset, bufferLength);
        }

        @Override
        public Optional<ColumnIndex> readColumnIndex(ColumnChunkMetaData column)
                throws IOException
        {
            return delegate.readColumnIndex(column);
        }

        @Override
        public Optional<OffsetIndex> readOffsetIndex(ColumnChunkMetaData column)
                throws IOException
        {
            return delegate.readOffsetIndex(column);
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }
    }
}
