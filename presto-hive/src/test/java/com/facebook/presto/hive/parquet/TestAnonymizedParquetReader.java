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

import com.facebook.airlift.testing.TempFile;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.testing.TestingConnectorSession;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.anonymization.TestAnonymizationManager;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.METASTORE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.benchmark.FileFormat.createPageSource;
import static com.facebook.presto.hive.parquet.ParquetTester.assertPageSource;
import static com.facebook.presto.hive.parquet.ParquetTester.assertRecordCursor;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAnonymizedParquetReader
{
    // number of rows to expand test data and test with
    private static final int TEST_ROWS = 9753;
    private static final List<String> COLUMN_NAMES = asList("id", "last_name");
    private static final List<com.facebook.presto.common.type.Type> COLUMN_TYPES = asList(INTEGER, VARCHAR);
    private static final List<List<?>> values = asList(
            asList(10, 20, null, 30),
            asList("Johnson", "Priyanka", "Zhou", "", null));
    private static final List<List<?>> expect = asList(
            asList(10, 20, null, 30),
            asList("Johnson", "Priyanka", "Zhou", "", null));
    private static final List<List<?>> anonymizedExpect = asList(
            asList(10, 20, null, 30),
            asList("J****", "P****", "Z****", "", null));

    @Test
    public void writeAndReadParquet() throws Exception
    {
        MessageType schema = new MessageType("schema",
                asList(new PrimitiveType(Type.Repetition.OPTIONAL, INT32, "id"),
                        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "last_name")));
        List<BiConsumer<Group, Object>> groupAdd =
                asList((g, v) -> g.add(COLUMN_NAMES.get(0), (Integer) v),
                        (g, v) -> g.add(COLUMN_NAMES.get(1), (String) v));
        // Expand provided values/expect to more rows
        List<Iterable<?>> testValues = values.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        List<Iterable<?>> testExpect = expect.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        List<Iterable<?>> testAnonymizedExpect = anonymizedExpect.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        try (TempFile tmp = new TempFile()) {
            File file = tmp.file();
            checkArgument(COLUMN_TYPES.size() == groupAdd.size());
            writeParquet(file, schema, groupAdd, iterators(testValues));
            // No anonymization, read should match original values
            assertRead(file, COLUMN_TYPES, COLUMN_NAMES, testExpect);
            // Read anonymized values for the last name column
            assertReadAnonymized(file, COLUMN_TYPES, COLUMN_NAMES, testAnonymizedExpect);
        }
    }

    @Test
    public void testAnonymizeNonStringColumns() throws Exception
    {
        MessageType schema = new MessageType("schema",
                asList(new PrimitiveType(Type.Repetition.OPTIONAL, INT32, "age")));
        List<BiConsumer<Group, Object>> groupAdd =
                asList((g, v) -> g.add("age", (Integer) v));
        List<List<?>> nonStringValues = asList(asList(10, 20, null, 30));
        List<List<?>> nonStringExpected = asList(asList(10, 20, null, 30));
        List<List<?>> nonStringAnonymizedExpected = asList(asList(10, 20, null, 30));
        // Expand provided values/expect to more rows
        List<Iterable<?>> testValues = nonStringValues.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        List<Iterable<?>> testExpect = nonStringExpected.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        List<Iterable<?>> testAnonymizedExpect = nonStringAnonymizedExpected.stream().map(column -> limit(cycle(column), TEST_ROWS))
                .collect(Collectors.toList());
        List<com.facebook.presto.common.type.Type> nonStringColumnTypes = asList(INTEGER);
        List<String> nonStringColumnNames = asList("age");
        try (TempFile tmp = new TempFile()) {
            File file = tmp.file();
            checkArgument(nonStringColumnTypes.size() == groupAdd.size());
            writeParquet(file, schema, groupAdd, iterators(testValues));
            // No anonymization, read should match original values
            assertRead(file, nonStringColumnTypes, nonStringColumnNames, testExpect);
            // Read anonymized values for the last name column
            assertReadAnonymized(file, nonStringColumnTypes, nonStringColumnNames, testAnonymizedExpect);
        }
        catch (UnsupportedOperationException e) {
            // Verify the exception message
            assertEquals(e.getMessage(), "Only string column anonymization is supported. Column age is of type INT32");
        }
    }

    void writeParquet(
            File outputFile,
            MessageType schema,
            List<BiConsumer<Group, Object>> groupAdd,
            Iterator<?>[] values)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        GroupWriteSupport.setSchema(schema, jobConf);
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new Path(outputFile.toURI()))
                .withWriteMode(OVERWRITE)
                .withType(schema)
                .withCompressionCodec(ZSTD)
                .withConf(jobConf)
                .withDictionaryEncoding(true)
                .build()) {
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            while (stream(values).allMatch(Iterator::hasNext)) {
                Group group = groupFactory.newGroup();
                for (int i = 0; i < values.length; i++) {
                    Object value = values[i].next();
                    if (value == null) {
                        continue;
                    }
                    groupAdd.get(i).accept(group, value);
                }
                writer.write(group);
            }
        }
    }

    void assertRead(File file,
                    List<com.facebook.presto.common.type.Type> types,
                    List<String> columnNames, List<Iterable<?>> expect)
            throws Exception
    {
        HiveClientConfig config = new HiveClientConfig()
                .setHiveStorageFormat(HiveStorageFormat.PARQUET);
        HiveCommonClientConfig commonConfig = new HiveCommonClientConfig()
                .setUseParquetColumnNames(true)
                .setParquetMaxReadBlockSize(new DataSize(1_000, DataSize.Unit.BYTE));
        ConnectorSession session = new TestingConnectorSession(getAllSessionProperties(config, commonConfig));
        HiveBatchPageSourceFactory pageSourceFactory = getPageSourceFactory();
        try (ConnectorPageSource connectorPageSource = createPageSource(
                pageSourceFactory, session, file, columnNames, types, HiveStorageFormat.PARQUET, 0L)) {
            Iterator<?>[] expected = iterators(expect);
            if (connectorPageSource instanceof RecordPageSource) {
                assertRecordCursor(
                        types, expected, ((RecordPageSource) connectorPageSource).getCursor());
            }
            else {
                assertPageSource(types, expected, connectorPageSource, Optional.empty());
            }
            assertFalse(stream(expected).anyMatch(Iterator::hasNext));
        }
    }

    void assertReadAnonymized(File file,
                              List<com.facebook.presto.common.type.Type> types,
                              List<String> columnNames, List<Iterable<?>> expect)
            throws Exception
    {
        HiveClientConfig config = new HiveClientConfig()
                .setHiveStorageFormat(HiveStorageFormat.PARQUET);
        HiveCommonClientConfig commonConfig = new HiveCommonClientConfig()
                .setUseParquetColumnNames(true)
                .setParquetMaxReadBlockSize(new DataSize(1_000, DataSize.Unit.BYTE))
                // Enable anonymization read
                .setParquetAnonymizationEnabled(true)
                .setParquetAnonymizationManagerClass(TestAnonymizationManager.class.getName());
        ConnectorSession session = new TestingConnectorSession(getAllSessionProperties(config, commonConfig));
        HiveBatchPageSourceFactory pageSourceFactory = getPageSourceFactory();
        try (ConnectorPageSource connectorPageSource = createPageSource(
                pageSourceFactory, session, file, columnNames, types, HiveStorageFormat.PARQUET, 0L)) {
            Iterator<?>[] expected = iterators(expect);
            if (connectorPageSource instanceof RecordPageSource) {
                assertRecordCursor(
                        types, expected, ((RecordPageSource) connectorPageSource).getCursor());
            }
            else {
                assertPageSource(types, expected, connectorPageSource, Optional.empty());
            }
            assertFalse(stream(expected).anyMatch(Iterator::hasNext));
        }
    }

    Iterator<?>[] iterators(List<Iterable<?>> values)
    {
        return values.stream().map(Iterable::iterator).toArray(Iterator[]::new);
    }

    HiveBatchPageSourceFactory getPageSourceFactory()
    {
        // testHdfsEnvironment from this test suite adds auth check
        return new ParquetPageSourceFactory(
                FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, createTestHdfsEnvironment(HIVE_CLIENT_CONFIG, METASTORE_CLIENT_CONFIG),
                new FileFormatDataSourceStats(), new MetadataReader());
    }
}
