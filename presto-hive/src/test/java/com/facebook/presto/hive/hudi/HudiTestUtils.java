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

package com.facebook.presto.hive.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class HudiTestUtils
{
    private static final Logger log = Logger.get(HudiTestUtils.class);

    public static final String DATA_DIR = "data";

    private HudiTestUtils() {}

    public static DistributedQueryRunner createQueryRunner(
            Map<String, String> serverConfig,
            Map<String, String> connectorConfig,
            Function<Optional<ExtendedHiveMetastore>, Plugin> connectorPluginFactory,
            String connectorName,
            String catalogName,
            String defaultSchema)
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(catalogName).setSchema(defaultSchema).build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(serverConfig).build();

        // setup file metastore
        Path catalogDir = queryRunner.getCoordinator().getBaseDataDir().resolve("catalog");
        ExtendedHiveMetastore metastore = createFileHiveMetastore(catalogDir.toString());

        // prepare testing data
        Path testingDataDir = queryRunner.getCoordinator().getBaseDataDir().resolve(DATA_DIR);
        HudiTestingDataGenerator generator = new HudiTestingDataGenerator(metastore, defaultSchema, testingDataDir);
        generator.generateData();
        generator.generateMetadata();

        queryRunner.installPlugin(connectorPluginFactory.apply(Optional.of(metastore)));
        queryRunner.createCatalog(catalogName, connectorName, connectorConfig);

        log.info("Using %s as catalog directory ", catalogDir);
        log.info("Using %s as testing data directory", testingDataDir);
        return queryRunner;
    }

    private static ExtendedHiveMetastore createFileHiveMetastore(String catalogDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, catalogDir, "test");
    }
}
