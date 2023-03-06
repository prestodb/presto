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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestEncryption
{
    private final Configuration conf = new Configuration(false);

    @Test
    public void testBasicDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withExtraMeta(extraMetadata)
                .withPageSize(1000)
                .withDictionaryEnabled()
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testAllColumnsDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"id", "bal", "name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .withDictionaryEnabled()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testNoColumnsDecryption()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withDictionaryEnabled()
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testOneRecord()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(1)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withDictionaryEnabled()
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testMillionRows()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(1000000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withDictionaryEnabled()
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testPlainTextFooter()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("SNAPPY")
                .withDictionaryEnabled()
                .withPageSize(1000)
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testLargePageSize()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(100000)
                .withCodec("GZIP")
                .withPageSize(100000)
                .withDictionaryEnabled()
                .withFooterEncryption()
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testAesGcmCtr()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(100000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withDictionaryEnabled()
                .withEncrytionAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .build();
        decryptAndValidate(inputFile);
    }

    @Test
    public void testDataMaskingSingleColumn()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name"};
        String[] maskingColumn = {"name"};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withPageSize(1000)
                .withFooterEncryption()
                .withDataMaskingTest()
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingMultipleColumns()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withCodec("GZIP")
                .withExtraMeta(extraMetadata)
                .withPageSize(1000)
                .withFooterEncryption()
                .withDataMaskingTest()
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingEncryptedFooter()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .withFooterEncryption()
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingPlaintextFooter()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingGcmCtr()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .withEncrytionAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingLargePage()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .withPageSize(100000)
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingOneRecord()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"name", "gender"};
        String[] maskingColumn = {"name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(1)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    @Test
    public void testDataMaskingAllColumns()
            throws IOException
    {
        MessageType schema = createSchema();
        String[] encryptColumns = {"id", "name", "gender"};
        String[] maskingColumn = {"id", "name", "gender"};
        Map<String, String> extraMetadata = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
            }};
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withEncryptColumns(encryptColumns)
                .withNumRecord(10000)
                .withDataMaskingTest()
                .withCodec("GZIP")
                .build();
        validateMasking(inputFile, maskingColumn);
    }

    private MessageType createSchema()
    {
        return new MessageType("schema",
                new PrimitiveType(OPTIONAL, INT64, "id"),
                new PrimitiveType(OPTIONAL, INT32, "bal"),
                new PrimitiveType(REQUIRED, BINARY, "name"),
                new PrimitiveType(OPTIONAL, BINARY, "gender"));
    }

    private void decryptAndValidate(TestFile inputFile)
            throws IOException
    {
        Path path = new Path(inputFile.getFileName());
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        Optional<InternalFileDecryptor> fileDecryptor = createFileDecryptor();
        ParquetDataSource dataSource = new MockParquetDataSource(new ParquetDataSourceId(path.toString()), inputStream);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, inputFile.getFileSize(), fileDecryptor, false).getParquetMetadata();
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
        ParquetReader parquetReader = createParquetReader(parquetMetadata, messageColumn, dataSource, fileDecryptor);
        validateFile(parquetReader, messageColumn, inputFile);
    }

    private void validateMasking(TestFile inputFile, String[] maskingColumn)
            throws IOException
    {
        Path path = new Path(inputFile.getFileName());
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        Optional<InternalFileDecryptor> fileDecryptor = createFileDecryptor();
        ParquetDataSource dataSource = new MockParquetDataSource(new ParquetDataSourceId(path.toString()), inputStream);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, inputFile.getFileSize(), fileDecryptor, true).getParquetMetadata();
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
        ParquetReader parquetReader = createParquetReader(parquetMetadata, messageColumn, dataSource, fileDecryptor);
        validateFile(parquetReader, messageColumn, inputFile, maskingColumn);
    }

    private Optional<InternalFileDecryptor> createFileDecryptor()
    {
        FileDecryptionProperties fileDecryptionProperties = EncryptDecryptUtil.getFileDecryptionProperties();
        if (fileDecryptionProperties != null) {
            return Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
        }
        return Optional.empty();
    }

    private static void validateFile(ParquetReader parquetReader, MessageColumnIO messageColumn, TestFile inputFile)
            throws IOException
    {
        String[] maskingColumn = {};
        validateFile(parquetReader, messageColumn, inputFile, maskingColumn);
    }

    private static void validateFile(ParquetReader parquetReader, MessageColumnIO messageColumn, TestFile inputFile, String[] maskingColumn)
            throws IOException
    {
        int rowIndex = 0;
        int batchSize = parquetReader.nextBatch();
        while (batchSize > 0) {
            validateColumn("id", BIGINT, rowIndex, parquetReader, messageColumn, inputFile, maskingColumn);
            validateColumn("bal", INTEGER, rowIndex, parquetReader, messageColumn, inputFile, maskingColumn);
            validateColumn("name", VARCHAR, rowIndex, parquetReader, messageColumn, inputFile, maskingColumn);
            validateColumn("gender", VARCHAR, rowIndex, parquetReader, messageColumn, inputFile, maskingColumn);
            rowIndex += batchSize;
            batchSize = parquetReader.nextBatch();
        }
    }

    @VisibleForTesting
    static void validateColumn(String name, Type type, int rowIndex, ParquetReader parquetReader, MessageColumnIO messageColumn, TestFile inputFile, String[] maskingColumn)
            throws IOException
    {
        HashSet<String> maskingColumnSet = new HashSet<>(Arrays.asList(maskingColumn));
        if (maskingColumnSet.contains(name)) {
            Field columnIO = constructField(type, lookupColumnByName(messageColumn, name)).orElse(null);
            assertNull(columnIO);
        }
        else {
            Block block = parquetReader.readBlock(constructField(type, lookupColumnByName(messageColumn, name)).orElse(null));
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (type.equals(BIGINT)) {
                    assertEquals(inputFile.getFileContent()[rowIndex++].getLong(name, 0), block.getLong(i));
                }
                else if (type.equals(INT32)) {
                    assertEquals(inputFile.getFileContent()[rowIndex++].getInteger(name, 0), block.getInt(i));
                }
                else if (type.equals(VARCHAR)) {
                    assertEquals(inputFile.getFileContent()[rowIndex++].getString(name, 0), block.getSlice(i, 0, block.getSliceLength(i)).toStringUtf8());
                }
            }
        }
    }

    @VisibleForTesting
    static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                String name = rowField.getName().get().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
        return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
    }

    @VisibleForTesting
    static ParquetReader createParquetReader(ParquetMetadata parquetMetadata,
            MessageColumnIO messageColumn,
            ParquetDataSource dataSource,
            Optional<InternalFileDecryptor> fileDecryptor)
    {
        return createParquetReader(parquetMetadata, messageColumn, dataSource, fileDecryptor, new DataSize(100000, DataSize.Unit.BYTE));
    }

    @VisibleForTesting
    static ParquetReader createParquetReader(ParquetMetadata parquetMetadata,
            MessageColumnIO messageColumn,
            ParquetDataSource dataSource,
            Optional<InternalFileDecryptor> fileDecryptor,
            DataSize maxReadBlockSize)
    {
        ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
        ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();

        long nextStart = 0;
        for (BlockMetaData block : parquetMetadata.getBlocks()) {
            blocks.add(block);
            blockStarts.add(nextStart);
            nextStart += block.getRowCount();
        }

        return new ParquetReader(
                messageColumn,
                blocks.build(),
                Optional.empty(),
                dataSource,
                com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                maxReadBlockSize,
                false,
                false,
                null,
                null,
                false,
                fileDecryptor);
    }
}
