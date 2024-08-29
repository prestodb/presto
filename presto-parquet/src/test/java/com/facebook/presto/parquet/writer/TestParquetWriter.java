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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.rowBlockOf;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestParquetWriter
{
    private File temporaryDirectory;
    private File parquetFile;

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

    @Test
    public void testMetadataCreation()
            throws Exception
    {
        temporaryDirectory = createTempDir();
        parquetFile = new File(temporaryDirectory, randomUUID().toString() + ".parquet");

        List<Type> types = ImmutableList.of(
                TINYINT,
                SMALLINT,
                BIGINT,
                INTEGER,
                DOUBLE,
                VARCHAR,
                BOOLEAN,
                SHORT_DECIMAL,
                LONG_DECIMAL,
                DATE,
                TIMESTAMP,
                MAP,
                ROW);

        List<String> names = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            names.add(Integer.toString(i));
        }

        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(DataSize.succinctBytes(1000))
                .setMaxBlockSize(DataSize.succinctBytes(15000))
                .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                .build();

        try (ParquetWriter parquetWriter = createParquetWriter(parquetFile, types, names, parquetWriterOptions, CompressionCodecName.UNCOMPRESSED)) {
            PageBuilder pageBuilder = new PageBuilder(100, types);
            TINYINT.writeLong(pageBuilder.getBlockBuilder(0), 10);
            SMALLINT.writeLong(pageBuilder.getBlockBuilder(1), 10);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(2), 10);
            INTEGER.writeLong(pageBuilder.getBlockBuilder(3), 10);
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), 10);
            VARCHAR.writeString(pageBuilder.getBlockBuilder(5), "10");
            BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(6), true);
            SHORT_DECIMAL.writeLong(pageBuilder.getBlockBuilder(7), 10);
            LONG_DECIMAL.writeSlice(pageBuilder.getBlockBuilder(8), Slices.utf8Slice("0000000000000000"));
            DATE.writeLong(pageBuilder.getBlockBuilder(9), 100);
            TIMESTAMP.writeLong(pageBuilder.getBlockBuilder(10), 100);
            MAP.writeObject(pageBuilder.getBlockBuilder(11), mapBlockOf(VARCHAR, VARCHAR, "1", "1"));
            ROW.writeObject(pageBuilder.getBlockBuilder(12), rowBlockOf(ImmutableList.of(VARCHAR), "1"));
            pageBuilder.declarePosition();

            parquetWriter.write(pageBuilder.build());
            parquetWriter.close();
            FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetFile.length(),
                    Optional.empty(),
                    false).getParquetMetadata();
            MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
            List<org.apache.parquet.schema.Type> parquetTypes = parquetSchema.getFields();

            List<String> expectedPrimitiveTypeNames = ImmutableList.of(
                    "INT32",
                    "INT32",
                    "INT64",
                    "INT32",
                    "DOUBLE",
                    "BINARY",
                    "BOOLEAN",
                    "INT64",
                    "FIXED_LEN_BYTE_ARRAY",
                    "INT32",
                    "INT64",
                    "",
                    "");

            List<Class<?>> expectedLogicalTypeAnnotationTypes = new ArrayList<>();
            for (int i = 0; i < 7; i++) {
                expectedLogicalTypeAnnotationTypes.add(null);
            }
            expectedLogicalTypeAnnotationTypes.add(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class);
            expectedLogicalTypeAnnotationTypes.add(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.class);
            expectedLogicalTypeAnnotationTypes.add(LogicalTypeAnnotation.DateLogicalTypeAnnotation.class);
            expectedLogicalTypeAnnotationTypes.add(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class);
            expectedLogicalTypeAnnotationTypes.add(LogicalTypeAnnotation.MapLogicalTypeAnnotation.class);
            expectedLogicalTypeAnnotationTypes.add(null);

            for (int i = 0; i< parquetTypes.size(); i++) {
                checkTypes(parquetTypes.get(i), expectedLogicalTypeAnnotationTypes.get(i), expectedPrimitiveTypeNames.get(i));
            }

            // check logical type parameters
            org.apache.parquet.schema.Type shortDecimalType = parquetTypes.get(7);
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation shortDecimalTypeAnnotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) shortDecimalType.getLogicalTypeAnnotation();
            assertEquals(shortDecimalTypeAnnotation.getScale(), SHORT_DECIMAL.getScale());
            assertEquals(shortDecimalTypeAnnotation.getPrecision(), SHORT_DECIMAL.getPrecision());

            org.apache.parquet.schema.Type longDecimalType = parquetTypes.get(8);
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation longDecimalTypeAnnotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) longDecimalType.getLogicalTypeAnnotation();
            assertEquals(longDecimalTypeAnnotation.getScale(), LONG_DECIMAL.getScale());
            assertEquals(longDecimalTypeAnnotation.getPrecision(), LONG_DECIMAL.getPrecision());

            org.apache.parquet.schema.Type timestampType = parquetTypes.get(10);
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timeAnnotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) timestampType.getLogicalTypeAnnotation();
            assertEquals(timeAnnotation.getUnit(), LogicalTypeAnnotation.TimeUnit.MILLIS);
            assertFalse(timeAnnotation.isAdjustedToUTC());
        }
    }

    private static void checkTypes(org.apache.parquet.schema.Type type, Class<?> expectedLogicalTypeAnnotationType, String expectedPrimitiveTypeName)
    {
        try {
            PrimitiveType primitiveType = type.asPrimitiveType();
            assertEquals(primitiveType.getPrimitiveTypeName().name(), expectedPrimitiveTypeName);
        }
        catch (ClassCastException e) {
            assertEquals(expectedPrimitiveTypeName, "");
        }

        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (annotation != null) {
            assertTrue(expectedLogicalTypeAnnotationType.isInstance(annotation));
        }
        else {
            assertNull(expectedLogicalTypeAnnotationType);
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
                new FileOutputStream(outputFile),
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
