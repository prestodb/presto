package com.facebook.presto.iceberg;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.cache.CachingModule;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveCommonModule;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.gcs.HiveGcsModule;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastoreConfig;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.iceberg.rest.IcebergRestTestUtil;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import javax.inject.Inject;
import javax.management.MBeanServer;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNamesWithEmptyVersion;
import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.google.common.io.Files.createTempDir;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class TestIcebergFileWriter {

    private IcebergFileWriterFactory icebergFileWriterFactory;

    @BeforeClass
    public void setup()
    {
        ConnectorContext context = new TestingConnectorContext();
        String catalogName = "testing_catalog";
        Bootstrap app = new Bootstrap(
                new EventModule(),
                new MBeanModule(),
                new JsonModule(),
                new IcebergCommonModule(catalogName),
//                new IcebergCatalogModule(catalogName, Optional.of(new FileHiveMetastore(
//                        getHdfsEnvironment(),
//                        new FileHiveMetastoreConfig()))),
                new HiveS3Module(catalogName),
                new HiveGcsModule(),
                new HiveAuthenticationModule(),
                new CachingModule(),
                new HiveCommonModule(),
                binder -> {
                    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                    binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                    binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                    binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                    binder.bind(FilterStatsCalculatorService.class).toInstance(context.getFilterStatsCalculatorService());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.of())
                .initialize();

        //this.icebergFileWriterFactory = injector.getInstance(IcebergFileWriterFactory.class);
//        this.icebergFileWriterFactory = new IcebergFileWriterFactory(
//                getHdfsEnvironment(),
//                new
//        );
    }

    @Test
    public void testWriteParquetFileWithLogicalTypes()
    {
        File temporaryDirectory = createTempDir();
        File parquetFile = new File(temporaryDirectory, randomUUID().toString());

        Schema icebergSchema = new Schema(Types.NestedField.optional(0, "timestamp_micros", Types.TimestampType.withoutZone()));

        icebergFileWriterFactory.createFileWriter(
                new Path(parquetFile.getPath()),
                icebergSchema,
                new JobConf(),
                SESSION,
                new HdfsContext(SESSION),
                FileFormat.PARQUET,
                MetricsConfig.getDefault());

        try {
            FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetFile.length(),
                    Optional.empty(),
                    false).getParquetMetadata();
            MessageType writtenSchema = parquetMetadata.getFileMetaData().getSchema();
            assertEquals(convert(icebergSchema, "table"), writtenSchema);
        }
        catch (IOException e) {
            fail("failed to read back file metadata", e);
        }
    }
}
