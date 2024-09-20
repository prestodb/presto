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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestParquetWriter
{
    private File temporaryDirectory;
    private File parquetFile;

    private static final DecimalType VERY_SHORT_DECIMAL = DecimalType.createDecimalType(1);
    private static final DecimalType SHORT_DECIMAL = DecimalType.createDecimalType(18);
    private static final DecimalType LONG_DECIMAL = DecimalType.createDecimalType(38);
    private static final Type MAP = new MapType(
            VARCHAR,
            VARCHAR,
            nativeValueGetter(VARCHAR),
            nativeValueGetter(VARCHAR));
    private static final Type ROW = RowType.from(ImmutableList.of(RowType.field("varchar", VARCHAR)));

    @Test
    public void testRowGroupFlushInterleavedColumnWriterFallbacks()
    {
        temporaryDirectory = createTempDir();
        parquetFile = new File(temporaryDirectory, randomUUID().toString());
        List<Type> types = ImmutableList.of(BIGINT, INTEGER, VARCHAR, BOOLEAN);
        List<String> names = ImmutableList.of("col_1", "col_2", "col_3", "col_4");
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(DataSize.succinctBytes(1000))
                .setMaxBlockSize(DataSize.succinctBytes(15000))
                .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                .build();
        try (ParquetWriter parquetWriter = createParquetWriter(parquetFile, types, names, parquetWriterOptions, CompressionCodecName.UNCOMPRESSED)) {
            Random rand = new Random();
            for (int pageIdx = 0; pageIdx < 10; pageIdx++) {
                int pageRowCount = 100;
                PageBuilder pageBuilder = new PageBuilder(pageRowCount, types);
                for (int rowIdx = 0; rowIdx < pageRowCount; rowIdx++) {
                    // maintain col_1's dictionary size approximately half of raw data
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), pageIdx * 100 + rand.nextInt(50));
                    INTEGER.writeLong(pageBuilder.getBlockBuilder(1), rand.nextInt(100000000));
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(2), randomUUID().toString());
                    BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(3), rand.nextBoolean());
                    pageBuilder.declarePosition();
                }
                parquetWriter.write(pageBuilder.build());
            }
        }
        catch (Exception e) {
            fail("Should not fail, but throw an exception as follows:", e);
        }
    }

    @DataProvider(name = "testMetadataCreation")
    public static Object[][] types()
    {
        return new Object[][] {
                {TINYINT, null, "INT32"},
                {SMALLINT, null, "INT32"},
                {BIGINT, null, "INT64"},
                {INTEGER, null, "INT32"},
                {DOUBLE, null, "DOUBLE"},
                {VARCHAR, null, "BINARY"},
                {BOOLEAN, null, "BOOLEAN"},
                {VERY_SHORT_DECIMAL, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class, "INT32"},
                {SHORT_DECIMAL, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class, "INT64"},
                {LONG_DECIMAL, LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class, "FIXED_LEN_BYTE_ARRAY"},
                {DATE, LogicalTypeAnnotation.DateLogicalTypeAnnotation.class, "INT32"},
                {TIMESTAMP, LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class, "INT64"},
                {MAP, LogicalTypeAnnotation.MapLogicalTypeAnnotation.class, null},
                {ROW, null, null}
        };
    }

    @Test(dataProvider = "testMetadataCreation")
    public void testMetadataCreation(Type type, Class<?> annotationType, String primitiveName)
            throws Exception
    {
        temporaryDirectory = createTempDir();
        parquetFile = new File(temporaryDirectory, randomUUID() + ".parquet");

        List<String> names = ImmutableList.of("col_1");
        List<Type> types = ImmutableList.of(type);

        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(DataSize.succinctBytes(1000))
                .setMaxBlockSize(DataSize.succinctBytes(15000))
                .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                .build();

        try (ParquetWriter parquetWriter = createParquetWriter(parquetFile, types, names, parquetWriterOptions, CompressionCodecName.UNCOMPRESSED)) {
            PageBuilder pageBuilder = new PageBuilder(0, types);
            pageBuilder.getBlockBuilder(0).appendNull();
            pageBuilder.declarePosition();
            parquetWriter.write(pageBuilder.build());
        }
        FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                dataSource,
                parquetFile.length(),
                Optional.empty(),
                false).getParquetMetadata();
        MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
        List<org.apache.parquet.schema.Type> parquetTypes = parquetSchema.getFields();

        checkTypes(parquetTypes.get(0), annotationType, primitiveName);

        if (type instanceof DecimalType) {
            org.apache.parquet.schema.Type decimalType = parquetTypes.get(0);
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalTypeAnnotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) decimalType.getLogicalTypeAnnotation();
            assertEquals(decimalTypeAnnotation.getScale(), ((DecimalType) type).getScale());
            assertEquals(decimalTypeAnnotation.getPrecision(), ((DecimalType) type).getPrecision());
        }
        else if (type instanceof TimestampType) {
            org.apache.parquet.schema.Type timestampType = parquetTypes.get(0);
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timeAnnotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) timestampType.getLogicalTypeAnnotation();
            assertEquals(timeAnnotation.getUnit(), LogicalTypeAnnotation.TimeUnit.MILLIS);
            assertFalse(timeAnnotation.isAdjustedToUTC());
        }
    }

    private static void checkTypes(org.apache.parquet.schema.Type type, Class<?> expectedAnnotationType, String expectedPrimitiveTypeName)
    {
        if (expectedPrimitiveTypeName != null) {
            PrimitiveType primitiveType = type.asPrimitiveType();
            assertEquals(primitiveType.getPrimitiveTypeName().name(), expectedPrimitiveTypeName);
        }
        else {
            assertThrows(type::asPrimitiveType);
        }

        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (expectedAnnotationType != null) {
            assertTrue(expectedAnnotationType.isInstance(annotation));
        }
        else {
            assertNull(annotation);
        }
    }

    public static ParquetWriter createParquetWriter(File outputFile, List<Type> types, List<String> columnNames,
            ParquetWriterOptions parquetWriterOptions, CompressionCodecName compressionCodecName)
            throws Exception
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                types,
                columnNames);
        return new ParquetWriter(
                Files.newOutputStream(outputFile.toPath()),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                columnNames,
                types,
                parquetWriterOptions,
                compressionCodecName.getHadoopCompressionCodecClassName());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
    }
}
