package com.facebook.presto.iceberg;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergFileWriter
    extends AbstractTestQueryFramework {

    private IcebergFileWriterFactory fileWriterFactory;

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
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA iceberg.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.nation (\n" +
                "    stamp TIMESTAMP,\n" +
                "    stamptz TIMESTAMPTZ,\n" +
                "    nested struct<TIMESTAMP>\n" +
                ")");

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());

        this.fileWriterFactory = new IcebergFileWriterFactory(
                hdfsEnvironment,
                MetadataManager.createTestMetadataManager().getFunctionAndTypeManager(),
                new FileFormatDataSourceStats(),
                new NodeVersion("test_version"),
                new OrcFileWriterConfig(),
                null
        );
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS iceberg.test_schema");
    }

    @Test
    public void testIcebergParquetTimestampWriter()
    {

    }
}
