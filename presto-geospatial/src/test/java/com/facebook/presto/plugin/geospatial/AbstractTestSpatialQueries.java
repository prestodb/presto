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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public abstract class AbstractTestSpatialQueries
        extends AbstractTestQueryFramework
{
    protected AbstractTestSpatialQueries()
    {
        super(() -> createQueryRunner());
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource(TestSpatialJoins.class.getSimpleName())
                .setCatalog("hive")
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .build();
        queryRunner.installPlugin(new GeoPlugin());

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());

        FileHiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
        metastore.createDatabase(Database.builder()
                .setDatabaseName("default")
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build());
        queryRunner.installPlugin(new HivePlugin("hive", Optional.of(metastore)));

        queryRunner.createCatalog("hive", "hive");
        return queryRunner;
    }
}
