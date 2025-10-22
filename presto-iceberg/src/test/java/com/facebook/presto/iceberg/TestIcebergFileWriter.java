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

package com.facebook.presto.iceberg;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toIcebergSchema;
import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.dataSizeSessionProperty;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.io.Files.createTempDir;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;
import static org.testng.Assert.assertEquals;

public class TestIcebergFileWriter
{
    private IcebergFileWriterFactory icebergFileWriterFactory;
    private HdfsContext hdfsContext;
    private ConnectorSession connectorSession;

    @BeforeClass
    public void setup()
            throws Exception
    {
        ConnectorId connectorId = new ConnectorId("iceberg");
        SessionPropertyManager sessionPropertyManager = createTestingSessionPropertyManager();

        sessionPropertyManager.addConnectorSessionProperties(
                connectorId,
                ImmutableList.of(
                        dataSizeSessionProperty("parquet_writer_page_size", "Parquet: Writer page size", new DataSize(10, DataSize.Unit.KILOBYTE), false),
                        dataSizeSessionProperty("parquet_writer_block_size", "Parquet: Writer block size", new DataSize(10, DataSize.Unit.KILOBYTE), false),
                        new PropertyMetadata<>(
                                "parquet_writer_version",
                                "Parquet: Writer version",
                                VARCHAR,
                                ParquetProperties.WriterVersion.class,
                                ParquetProperties.WriterVersion.PARQUET_2_0,
                                false,
                                value -> ParquetProperties.WriterVersion.valueOf(((String) value).toUpperCase()),
                                ParquetProperties.WriterVersion::name),
                        new PropertyMetadata<>(
                                "compression_codec",
                                "The compression codec to use when writing files",
                                VARCHAR,
                                HiveCompressionCodec.class,
                                HiveCompressionCodec.NONE,
                                false,
                                value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                                HiveCompressionCodec::name)));

        Session session = testSessionBuilder(sessionPropertyManager)
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        this.connectorSession = session.toConnectorSession(connectorId);
        TypeManager typeManager = new TestingTypeManager();
        this.hdfsContext = new HdfsContext(connectorSession);
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
        this.icebergFileWriterFactory = new IcebergFileWriterFactory(hdfsEnvironment, typeManager,
                new FileFormatDataSourceStats(), new NodeVersion("test"), new OrcFileWriterConfig(), HiveDwrfEncryptionProvider.NO_ENCRYPTION);
    }

    @Test
    public void testWriteParquetFileWithLogicalTypes()
            throws Exception
    {
        Path path = new Path(createTempDir().getAbsolutePath() + "/test.parquet");
        Schema icebergSchema = toIcebergSchema(ImmutableList.of(
                ColumnMetadata.builder().setName("a").setType(VARCHAR).build(),
                ColumnMetadata.builder().setName("b").setType(INTEGER).build(),
                ColumnMetadata.builder().setName("c").setType(TIMESTAMP).build(),
                ColumnMetadata.builder().setName("d").setType(DATE).build()));
        IcebergFileWriter icebergFileWriter = this.icebergFileWriterFactory.createFileWriter(path, icebergSchema, new JobConf(), connectorSession,
                hdfsContext, FileFormat.PARQUET, MetricsConfig.getDefault());

        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, TIMESTAMP, DATE)
                .addSequencePage(100, 0, 0, 123, 100)
                .addSequencePage(100, 100, 100, 223, 100)
                .addSequencePage(100, 200, 200, 323, 100)
                .build();
        for (Page page : input) {
            icebergFileWriter.appendRows(page);
        }
        icebergFileWriter.commit();

        File parquetFile = new File(path.toString());
        FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                dataSource,
                parquetFile.length(),
                Optional.empty(),
                false).getParquetMetadata();
        MessageType writtenSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageType originalSchema = convert(icebergSchema, "table");
        assertEquals(originalSchema, writtenSchema);
    }

    private static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                    return type;
                }
            }
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters));
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(BooleanType.BOOLEAN, INTEGER, BIGINT, DoubleType.DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, HYPER_LOG_LOG);
        }

        @Override
        public boolean hasType(TypeSignature signature)
        {
            return getType(signature) != null;
        }
    }
}
