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
package com.facebook.presto.hive.serde;

import com.facebook.presto.common.experimental.ConnectorOutputTableHandleAdapter;
import com.facebook.presto.common.experimental.ConnectorTransactionHandleAdapter;
import com.facebook.presto.common.experimental.ExecutionWriterTargetAdapter;
import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftExecutionWriterTarget;
import com.facebook.presto.common.experimental.auto_gen.ThriftOutputTableHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftTableWriteInfo;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveOutputTableHandle;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveWriterStats;
import com.facebook.presto.hive.LocationHandle;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.SortingFileWriterConfig;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePageSinkMetadata;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchColumnTypes;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.LocationHandle.TableType.NEW;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestConnectorOutputTableHandle
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("testConnectorId");
    private static final String SCHEMA_NAME = "testSchema";
    private static final String TABLE_NAME = "testTable";

    private static final TSerializer serializer;
    private static final TDeserializer deserializer;

    static {
        try {
            serializer = new TSerializer(new TJSONProtocol.Factory());
            deserializer = new TDeserializer(new TJSONProtocol.Factory());
        }
        catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestHiveTransactionHandleSerDe()
    {
        UUID testId = UUID.randomUUID();
        ConnectorTransactionHandle handle = new HiveTransactionHandle(testId);
        byte[] bytes = ConnectorTransactionHandleAdapter.serialize(handle);

        HiveTransactionHandle deserialized = (HiveTransactionHandle) ConnectorTransactionHandleAdapter.deserialize(bytes);
        assertEquals(handle, deserialized);
    }

    @Test
    public void TestHiveTransactionHandleInterfaceSerDe()
    {
        UUID testId = UUID.randomUUID();
        HiveTransactionHandle handle = new HiveTransactionHandle(testId);
        ThriftConnectorTransactionHandle thriftTransactionHandle = handle.toThriftInterface();
        byte[] bytes = FbThriftUtils.serialize(thriftTransactionHandle);

        ThriftConnectorTransactionHandle deserialized = FbThriftUtils.deserialize(ThriftConnectorTransactionHandle.class, bytes);
        HiveTransactionHandle newHandle = (HiveTransactionHandle) ConnectorTransactionHandleAdapter.fromThrift(deserialized);

        assertEquals(handle.getUuid(), newHandle.getUuid());
    }

    @Test
    public void TestHiveOutputTableHandle()
    {
        HiveClientConfig config = new HiveClientConfig();
        SortingFileWriterConfig sortingFileWriterConfig = new SortingFileWriterConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        File tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
        for (HiveStorageFormat format : getSupportedHiveStorageFormats()) {
            config.setHiveStorageFormat(format);
            config.setCompressionCodec(NONE);

            ConnectorTransactionHandle transaction = new HiveTransactionHandle();
            HiveWriterStats stats = new HiveWriterStats();
            String outputPath = makeFileName(tempDir, config);
            Path path = new Path("file:///" + outputPath);

            LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
            HiveOutputTableHandle hiveOutputTableHandle = new HiveOutputTableHandle(
                    SCHEMA_NAME,
                    TABLE_NAME,
                    getColumnHandles(),
                    new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(METASTORE_CONTEXT, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                    locationHandle,
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getCompressionCodec(),
                    ImmutableList.of(),
                    Optional.empty(),
                    ImmutableList.of(),
                    "test",
                    ImmutableMap.of(),
                    Optional.empty());

            byte[] bytes = ConnectorOutputTableHandleAdapter.serialize(hiveOutputTableHandle);

            HiveOutputTableHandle deserializedHandle = (HiveOutputTableHandle) ConnectorOutputTableHandleAdapter.deserialize(bytes);

            assertEquals(hiveOutputTableHandle.getTableOwner(), deserializedHandle.getTableOwner());
            assertEquals(hiveOutputTableHandle.getPreferredOrderingColumns(), deserializedHandle.getPreferredOrderingColumns());
            assertEquals(hiveOutputTableHandle.getSchemaTableName(), deserializedHandle.getSchemaTableName());
            assertEquals(hiveOutputTableHandle.getSchemaName(), deserializedHandle.getSchemaName());

            assertEquals(hiveOutputTableHandle.getTableStorageFormat(), deserializedHandle.getTableStorageFormat());
            assertEquals(hiveOutputTableHandle.getPartitionStorageFormat(), deserializedHandle.getPartitionStorageFormat());
            assertEquals(hiveOutputTableHandle.getActualStorageFormat(), deserializedHandle.getActualStorageFormat());

            assertEquals(hiveOutputTableHandle.getPartitionedBy(), deserializedHandle.getPartitionedBy());
            assertEquals(hiveOutputTableHandle.getTableName(), deserializedHandle.getTableName());
            assertEquals(hiveOutputTableHandle.getAdditionalTableParameters(), deserializedHandle.getAdditionalTableParameters());
            assertEquals(hiveOutputTableHandle.getBucketProperty(), deserializedHandle.getBucketProperty());
            assertEquals(hiveOutputTableHandle.getInputColumns(), deserializedHandle.getInputColumns());
            assertEquals(hiveOutputTableHandle.getCompressionCodec(), deserializedHandle.getCompressionCodec());
            assertEquals(hiveOutputTableHandle.getEncryptionInformation(), deserializedHandle.getEncryptionInformation());

            assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getTable(), deserializedHandle.getPageSinkMetadata().getTable());
            assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getSchemaTableName(), deserializedHandle.getPageSinkMetadata().getSchemaTableName());
            assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getModifiedPartitions(), deserializedHandle.getPageSinkMetadata().getModifiedPartitions());

            assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTableType(), deserializedHandle.getLocationHandle().getJsonSerializableTableType());
            assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableWriteMode(), deserializedHandle.getLocationHandle().getJsonSerializableWriteMode());
            assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTempPath(), deserializedHandle.getLocationHandle().getJsonSerializableTempPath());
            assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTargetPath(), deserializedHandle.getLocationHandle().getJsonSerializableTargetPath());
            assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableWritePath(), deserializedHandle.getLocationHandle().getJsonSerializableWritePath());
        }
    }

    @Test
    public void TestTableWriteInfo()
            throws TException
    {
        HiveClientConfig config = new HiveClientConfig();
        File tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
        for (HiveStorageFormat format : getSupportedHiveStorageFormats()) {
            config.setHiveStorageFormat(format);
            config.setCompressionCodec(NONE);

            ConnectorTransactionHandle transaction = new HiveTransactionHandle();
            HiveWriterStats stats = new HiveWriterStats();
            String outputPath = makeFileName(tempDir, config);

            LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
            HiveOutputTableHandle hiveOutputTableHandle = new HiveOutputTableHandle(
                    SCHEMA_NAME,
                    TABLE_NAME,
                    getColumnHandles(),
                    new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(METASTORE_CONTEXT, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                    locationHandle,
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getCompressionCodec(),
                    ImmutableList.of(),
                    Optional.empty(),
                    ImmutableList.of(),
                    "test",
                    ImmutableMap.of(),
                    Optional.empty());

            OutputTableHandle outputTableHandle = new OutputTableHandle(CONNECTOR_ID, transaction, hiveOutputTableHandle);

            ExecutionWriterTarget handle = new ExecutionWriterTarget.CreateHandle(outputTableHandle,
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME));

            TableWriteInfo tableWriteInfo = new TableWriteInfo(Optional.of(handle), Optional.empty());

            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            byte[] bytes = FbThriftUtils.serialize(tableWriteInfo.toThrift());

            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            ThriftTableWriteInfo thriftInfo = FbThriftUtils.deserialize(ThriftTableWriteInfo.class, bytes);

            TableWriteInfo convertedBack = new TableWriteInfo(thriftInfo);

            ExecutionWriterTarget.CreateHandle createHandle = (ExecutionWriterTarget.CreateHandle) convertedBack.getWriterTarget().get();
            ExecutionWriterTarget.CreateHandle originalHandle = (ExecutionWriterTarget.CreateHandle) tableWriteInfo.getWriterTarget().get();
            assertEquals(createHandle.getSchemaTableName(), originalHandle.getSchemaTableName());
            assertEquals(createHandle.getHandle().getConnectorId(), originalHandle.getHandle().getConnectorId());

            OutputTableHandle createOutputHandle = createHandle.getHandle();
            OutputTableHandle originalOutputHandle = originalHandle.getHandle();

            assertEquals(outputTableHandle.getConnectorId(), originalOutputHandle.getConnectorId());

            HiveOutputTableHandle createHiveHandle = (HiveOutputTableHandle) createOutputHandle.getConnectorHandle();
            HiveOutputTableHandle originalHiveHandle = (HiveOutputTableHandle) originalOutputHandle.getConnectorHandle();

            assertHiveOutputTable(createHiveHandle, originalHiveHandle);

            HiveTransactionHandle createTransactionHandle = (HiveTransactionHandle) createOutputHandle.getTransactionHandle();
            HiveTransactionHandle originalTransactionHandle = (HiveTransactionHandle) originalOutputHandle.getTransactionHandle();

            assertEquals(createTransactionHandle.getUuid(), originalTransactionHandle.getUuid());
        }
    }

    @Test
    public void TestCreateHandle()
            throws TException
    {
        HiveClientConfig config = new HiveClientConfig();
        File tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
        for (HiveStorageFormat format : getSupportedHiveStorageFormats()) {
            config.setHiveStorageFormat(format);
            config.setCompressionCodec(NONE);

            ConnectorTransactionHandle transaction = new HiveTransactionHandle();
            HiveWriterStats stats = new HiveWriterStats();
            String outputPath = makeFileName(tempDir, config);
            Path path = new Path("file:///" + outputPath);

            LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
            HiveOutputTableHandle hiveOutputTableHandle = new HiveOutputTableHandle(
                    SCHEMA_NAME,
                    TABLE_NAME,
                    getColumnHandles(),
                    new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(METASTORE_CONTEXT, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                    locationHandle,
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getCompressionCodec(),
                    ImmutableList.of(),
                    Optional.empty(),
                    ImmutableList.of(),
                    "test",
                    ImmutableMap.of(),
                    Optional.empty());

            OutputTableHandle outputTableHandle = new OutputTableHandle(CONNECTOR_ID, transaction, hiveOutputTableHandle);

            ExecutionWriterTarget handle = new ExecutionWriterTarget.CreateHandle(outputTableHandle,
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME));

            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            byte[] bytes = FbThriftUtils.serialize(handle.toThriftInterface());

            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            ThriftExecutionWriterTarget thriftHandle = FbThriftUtils.deserialize(ThriftExecutionWriterTarget.class, bytes);

            ExecutionWriterTarget.CreateHandle createHandle = (ExecutionWriterTarget.CreateHandle) ExecutionWriterTargetAdapter.fromThrift(thriftHandle);
            ExecutionWriterTarget.CreateHandle originalHandle = (ExecutionWriterTarget.CreateHandle) handle;
            assertEquals(createHandle.getSchemaTableName(), originalHandle.getSchemaTableName());
            assertEquals(createHandle.getHandle().getConnectorId(), originalHandle.getHandle().getConnectorId());

            OutputTableHandle createOutputHandle = createHandle.getHandle();
            OutputTableHandle originalOutputHandle = originalHandle.getHandle();

            assertEquals(outputTableHandle.getConnectorId(), originalOutputHandle.getConnectorId());

            HiveOutputTableHandle createHiveHandle = (HiveOutputTableHandle) createOutputHandle.getConnectorHandle();
            HiveOutputTableHandle originalHiveHandle = (HiveOutputTableHandle) originalOutputHandle.getConnectorHandle();

            assertHiveOutputTable(createHiveHandle, originalHiveHandle);

            HiveTransactionHandle createTransactionHandle = (HiveTransactionHandle) createOutputHandle.getTransactionHandle();
            HiveTransactionHandle originalTransactionHandle = (HiveTransactionHandle) originalOutputHandle.getTransactionHandle();

            assertEquals(createTransactionHandle.getUuid(), originalTransactionHandle.getUuid());
        }
    }

    @Test
    public void TestOutputTableHandle()
            throws TException
    {
        HiveClientConfig config = new HiveClientConfig();
        File tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
        for (HiveStorageFormat format : getSupportedHiveStorageFormats()) {
            config.setHiveStorageFormat(format);
            config.setCompressionCodec(NONE);

            ConnectorTransactionHandle transaction = new HiveTransactionHandle();
            HiveWriterStats stats = new HiveWriterStats();
            String outputPath = makeFileName(tempDir, config);

            LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, Optional.empty(), NEW, DIRECT_TO_TARGET_NEW_DIRECTORY);
            HiveOutputTableHandle hiveOutputTableHandle = new HiveOutputTableHandle(
                    SCHEMA_NAME,
                    TABLE_NAME,
                    getColumnHandles(),
                    new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(METASTORE_CONTEXT, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                    locationHandle,
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getHiveStorageFormat(),
                    config.getCompressionCodec(),
                    ImmutableList.of(),
                    Optional.empty(),
                    ImmutableList.of(),
                    "test",
                    ImmutableMap.of(),
                    Optional.empty());

            OutputTableHandle outputTableHandle = new OutputTableHandle(CONNECTOR_ID, transaction, hiveOutputTableHandle);
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            byte[] bytes = FbThriftUtils.serialize(outputTableHandle.toThrift());

            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            ThriftOutputTableHandle thriftHandle = FbThriftUtils.deserialize(ThriftOutputTableHandle.class, bytes);

            OutputTableHandle convertedBack = new OutputTableHandle(thriftHandle);

            HiveTransactionHandle transactionHandle = (HiveTransactionHandle) convertedBack.getTransactionHandle();
            assertEquals(transactionHandle.getUuid(), ((HiveTransactionHandle) transaction).getUuid());

            HiveOutputTableHandle deserializedHandle = (HiveOutputTableHandle) convertedBack.getConnectorHandle();

            assertHiveOutputTable(hiveOutputTableHandle, deserializedHandle);
        }
    }

    private void assertHiveOutputTable(HiveOutputTableHandle hiveOutputTableHandle, HiveOutputTableHandle deserializedHandle)
    {
        assertEquals(hiveOutputTableHandle.getTableOwner(), deserializedHandle.getTableOwner());
        assertEquals(hiveOutputTableHandle.getPreferredOrderingColumns(), deserializedHandle.getPreferredOrderingColumns());
        assertEquals(hiveOutputTableHandle.getSchemaTableName(), deserializedHandle.getSchemaTableName());
        assertEquals(hiveOutputTableHandle.getSchemaName(), deserializedHandle.getSchemaName());

        assertEquals(hiveOutputTableHandle.getTableStorageFormat(), deserializedHandle.getTableStorageFormat());
        assertEquals(hiveOutputTableHandle.getPartitionStorageFormat(), deserializedHandle.getPartitionStorageFormat());
        assertEquals(hiveOutputTableHandle.getActualStorageFormat(), deserializedHandle.getActualStorageFormat());

        assertEquals(hiveOutputTableHandle.getPartitionedBy(), deserializedHandle.getPartitionedBy());
        assertEquals(hiveOutputTableHandle.getTableName(), deserializedHandle.getTableName());
        assertEquals(hiveOutputTableHandle.getAdditionalTableParameters(), deserializedHandle.getAdditionalTableParameters());
        assertEquals(hiveOutputTableHandle.getBucketProperty(), deserializedHandle.getBucketProperty());
        assertEquals(hiveOutputTableHandle.getInputColumns(), deserializedHandle.getInputColumns());
        assertEquals(hiveOutputTableHandle.getCompressionCodec(), deserializedHandle.getCompressionCodec());
        assertEquals(hiveOutputTableHandle.getEncryptionInformation(), deserializedHandle.getEncryptionInformation());

        assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getTable(), deserializedHandle.getPageSinkMetadata().getTable());
        assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getSchemaTableName(), deserializedHandle.getPageSinkMetadata().getSchemaTableName());
        assertEquals(hiveOutputTableHandle.getPageSinkMetadata().getModifiedPartitions(), deserializedHandle.getPageSinkMetadata().getModifiedPartitions());

        assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTableType(), deserializedHandle.getLocationHandle().getJsonSerializableTableType());
        assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableWriteMode(), deserializedHandle.getLocationHandle().getJsonSerializableWriteMode());
        assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTempPath(), deserializedHandle.getLocationHandle().getJsonSerializableTempPath());
        assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableTargetPath(), deserializedHandle.getLocationHandle().getJsonSerializableTargetPath());
        assertEquals(hiveOutputTableHandle.getLocationHandle().getJsonSerializableWritePath(), deserializedHandle.getLocationHandle().getJsonSerializableWritePath());
    }

    private static String makeFileName(File tempDir, HiveClientConfig config)
    {
        return tempDir.getAbsolutePath() + "/" + config.getHiveStorageFormat().name() + "." + config.getCompressionCodec().name();
    }

    protected List<HiveStorageFormat> getSupportedHiveStorageFormats()
    {
        // CSV supports only unbounded VARCHAR type, and Alpha does not support DML yet
        return Arrays.stream(HiveStorageFormat.values())
                .filter(format -> format != HiveStorageFormat.CSV && format != HiveStorageFormat.ALPHA)
                .collect(toImmutableList());
    }

    public static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            HiveType hiveType = getHiveType(column.getType());
            handles.add(new HiveColumnHandle(column.getColumnName(), hiveType, hiveType.getTypeSignature(), i, REGULAR, Optional.empty(), Optional.empty()));
        }
        return handles.build();
    }

    private static List<LineItemColumn> getTestColumns()
    {
        return Stream.of(LineItemColumn.values())
                // Not all the formats support DATE
                .filter(column -> !column.getType().equals(TpchColumnTypes.DATE))
                .collect(toList());
    }

    private static HiveType getHiveType(TpchColumnType type)
    {
        switch (type.getBase()) {
            case IDENTIFIER:
                return HIVE_LONG;
            case INTEGER:
                return HIVE_INT;
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
}
