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

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.airlift.testing.FileUtils;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.TpchColumnType;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHivePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String CLIENT_ID = "client_id";
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    public void testAllFormats()
            throws Exception
    {
        HiveClientConfig config = new HiveClientConfig();
        File tempDir = Files.createTempDir();
        try {
            HiveMetastore metastore = new InMemoryHiveMetastore(new File(tempDir, "metastore"));
            for (HiveStorageFormat format : HiveStorageFormat.values()) {
                config.setHiveStorageFormat(format);
                config.setHiveCompressionCodec(NONE);
                long uncompressedLength = writeTestFile(config, metastore, makeFileName(tempDir, config));
                assertGreaterThan(uncompressedLength, 0L);

                for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
                    if (codec == NONE) {
                        continue;
                    }
                    config.setHiveCompressionCodec(codec);
                    long length = writeTestFile(config, metastore, makeFileName(tempDir, config));
                    assertTrue(uncompressedLength > length, format("%s with %s compressed to %s which is not less than %s", format, codec, length, uncompressedLength));
                }
            }
        }
        finally {
            FileUtils.deleteRecursively(tempDir);
        }
    }

    private static String makeFileName(File tempDir, HiveClientConfig config)
    {
        return tempDir.getAbsolutePath() + "/" + config.getHiveStorageFormat().name() + "." + config.getHiveCompressionCodec().name();
    }

    private static long writeTestFile(HiveClientConfig config, HiveMetastore metastore, String outputPath)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        ConnectorPageSink pageSink = createPageSink(transaction, config, metastore, new Path("file:///" + outputPath));
        List<LineItemColumn> columns = getTestColumns();
        List<Type> columnTypes = columns.stream()
                .map(LineItemColumn::getType)
                .map(TestHivePageSink::getHiveType)
                .map(hiveType -> hiveType.getType(TYPE_MANAGER))
                .collect(toList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int rows = 0;
        for (LineItem lineItem :  new LineItemGenerator(0.01, 1, 1)) {
            rows++;
            if (rows >= NUM_ROWS) {
                break;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                LineItemColumn column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType()) {
                    case BIGINT:
                        BIGINT.writeLong(blockBuilder, column.getLong(lineItem));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(lineItem));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
                        break;
                    case VARCHAR:
                        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
        }
        Page page = pageBuilder.build();
        pageSink.appendPage(page, null);
        pageSink.finish();

        File outputDir = new File(outputPath);
        List<File> files = ImmutableList.copyOf(outputDir.listFiles((dir, name) -> !name.endsWith(".crc")));
        File outputFile = getOnlyElement(files);
        long length = outputFile.length();

        ConnectorPageSource pageSource = createPageSource(transaction, config, outputFile);

        List<Page> pages = new ArrayList<>();
        while (!pageSource.isFinished()) {
            Page nextPage = pageSource.getNextPage();
            if (nextPage != null) {
                nextPage.assureLoaded();
                pages.add(nextPage);
            }
        }
        MaterializedResult expectedResults = toMaterializedResult(getSession(config), columnTypes, ImmutableList.of(page));
        MaterializedResult results = toMaterializedResult(getSession(config), columnTypes, pages);
        assertEquals(results, expectedResults);
        return length;
    }

    public static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    private static ConnectorPageSource createPageSource(HiveTransactionHandle transaction, HiveClientConfig config, File outputFile)
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, config.getHiveStorageFormat().getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, config.getHiveStorageFormat().getSerDe());
        splitProperties.setProperty("columns", Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getName).collect(toList())));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getHiveType).map(HiveType::getHiveTypeName).collect(toList())));
        HiveSplit split = new HiveSplit(CLIENT_ID, SCHEMA_NAME, TABLE_NAME, "", "file:///" + outputFile.getAbsolutePath(), 0, outputFile.length(), splitProperties, ImmutableList.of(), ImmutableList.of(), OptionalInt.empty(), false, TupleDomain.all());
        HivePageSourceProvider provider = new HivePageSourceProvider(config, createHdfsEnvironment(config), getDefaultHiveRecordCursorProvider(config), getDefaultHiveDataStreamFactories(config), TYPE_MANAGER);
        return provider.createPageSource(transaction, getSession(config), split, ImmutableList.copyOf(getColumnHandles()));
    }

    private static ConnectorPageSink createPageSink(HiveTransactionHandle transaction, HiveClientConfig config, HiveMetastore metastore, Path outputPath)
    {
        LocationHandle locationHandle = new LocationHandle(outputPath, Optional.of(outputPath), false);
        HiveOutputTableHandle handle = new HiveOutputTableHandle(CLIENT_ID, SCHEMA_NAME, TABLE_NAME, getColumnHandles(), "test", locationHandle, config.getHiveStorageFormat(), config.getHiveStorageFormat(), ImmutableList.of(), Optional.empty(), "test", ImmutableMap.of());
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        HdfsEnvironment hdfsEnvironment = createHdfsEnvironment(config);
        HivePageSinkProvider provider = new HivePageSinkProvider(hdfsEnvironment, metastore, new GroupByHashPageIndexerFactory(), TYPE_MANAGER, config, new HiveLocationService(metastore, hdfsEnvironment), partitionUpdateCodec);
        return provider.createPageSink(transaction, getSession(config), handle);
    }

    private static TestingConnectorSession getSession(HiveClientConfig config)
    {
        return new TestingConnectorSession(new HiveSessionProperties(config).getSessionProperties());
    }

    private static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            HiveType hiveType = getHiveType(column.getType());
            handles.add(new HiveColumnHandle(CLIENT_ID, column.getColumnName(), hiveType, hiveType.getTypeSignature(), i, false));
        }
        return handles.build();
    }

    private static List<LineItemColumn> getTestColumns()
    {
        return Stream.of(LineItemColumn.values())
                // Not all the formats support DATE
                .filter(column -> column.getType() != TpchColumnType.DATE)
                .collect(toList());
    }

    private static HiveType getHiveType(TpchColumnType type)
    {
        switch (type) {
            case BIGINT:
                return HIVE_LONG;
            case DATE:
                return HIVE_DATE;
            case DOUBLE:
                return HIVE_DOUBLE;
            case VARCHAR:
                return HIVE_STRING;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static HdfsEnvironment createHdfsEnvironment(HiveClientConfig config)
    {
        return new HdfsEnvironment(new HiveHdfsConfiguration(new HdfsConfigurationUpdater(config)), config);
    }
}
