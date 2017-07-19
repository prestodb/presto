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
package com.facebook.presto.hive.unittests;

import com.facebook.presto.connector.meta.SupportedFeatures;
import com.facebook.presto.connector.unittest.BaseMetadataTest;
import com.facebook.presto.connector.unittest.MetadataSchemaTest;
import com.facebook.presto.connector.unittest.MetadataTableTest;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.HiveS3Config;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.connector.meta.ConnectorFeature.ADD_COLUMN;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.READ_DATA;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_COLUMN;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.WRITE_DATA;
import static com.facebook.presto.hive.HiveQueryRunner.TIME_ZONE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;

@SupportedFeatures({
        READ_DATA,
        WRITE_DATA,
        CREATE_SCHEMA,
        DROP_SCHEMA,
        RENAME_SCHEMA,
        CREATE_TABLE,
        CREATE_TABLE_AS,
        DROP_TABLE,
        RENAME_TABLE,
        ADD_COLUMN,
        RENAME_COLUMN})
public class TestHiveMetadata
        implements BaseMetadataTest, MetadataSchemaTest, MetadataTableTest
{
    private Connector connector;

    @BeforeAll
    public void beforeAll()
            throws Exception
    {
        this.connector = createHiveConnector();
    }

    @AfterAll
    public void cleanUp()
            throws Exception
    {
        connector.shutdown();
    }

    @Override
    public Connector getConnector()
    {
        return connector;
    }

    @Override
    public Map<String, Object> getTableProperties()
    {
        return ImmutableMap.of(
                BUCKETED_BY_PROPERTY, ImmutableList.of(),
                BUCKET_COUNT_PROPERTY, 0,
                PARTITIONED_BY_PROPERTY, ImmutableList.of(),
                STORAGE_FORMAT_PROPERTY, TEXTFILE);
    }

    @Override
    public List<ColumnMetadata> withSystemColumns(List<ColumnMetadata> connectorColumns)
    {
        return ImmutableList.<ColumnMetadata>builder()
                .addAll(connectorColumns)
                .add(new ColumnMetadata("$path", VARCHAR, null, true))
                .build();
    }

    private static Connector createHiveConnector()
            throws IOException
    {
        File baseDir = Files.createTempDirectory("PrestoTest").resolve("hive_data").toFile();

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        HiveS3Config s3Config = new HiveS3Config();

        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig, s3Config));
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());
        FileHiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");

        HivePlugin plugin = new HivePlugin("hive", metastore);
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://localhost:8080")
                .put("hive.time-zone", TIME_ZONE.getID())
                .build();

        ConnectorFactory factory = getOnlyElement(connectorFactories);
        return factory.create("hive", properties, new TestingConnectorContext());
    }
}
