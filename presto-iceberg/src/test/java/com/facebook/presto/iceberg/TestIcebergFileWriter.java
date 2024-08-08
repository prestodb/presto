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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.cache.CachingModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveCommonModule;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.RebindSafeMBeanServer;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.gcs.HiveGcsModule;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tests.AbstractTestQueryFramework;
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

import javax.management.MBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.google.common.io.Files.createTempDir;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestIcebergFileWriter
        extends AbstractTestQueryFramework
{
    private IcebergFileWriterFactory icebergFileWriterFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(
                        "iceberg.parquet.metadata-cache-enabled", "true",
                        "iceberg.parquet.metadata-cache-size", "100MB",
                        "iceberg.parquet.metadata-cache-ttl-since-last-access", "1h"),
                PARQUET,
                false,
                true,
                OptionalInt.of(2),
                Optional.empty());
    }

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
                new IcebergCatalogModule(catalogName, Optional.of(new FileHiveMetastore(
                        getHdfsEnvironment(),
                        "test_directory",
                        "test_user"))),
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

        this.icebergFileWriterFactory = injector.getInstance(IcebergFileWriterFactory.class);
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
