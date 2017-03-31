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

import com.facebook.presto.Session;
import com.facebook.presto.benchmark.BenchmarkSuite;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.TestingHiveMetastore;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class HiveBenchmarkQueryRunner
{
    private HiveBenchmarkQueryRunner()
    {
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = requireNonNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        File tempDir = Files.createTempDir();
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner(tempDir)) {
            new BenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
        finally {
            FileUtils.deleteRecursively(tempDir);
        }
    }

    public static LocalQueryRunner createLocalQueryRunner(File tempDir)
    {
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("tpch")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        // add hive
        File hiveDir = new File(tempDir, "hive_data");
        TestingHiveMetastore metastore = new TestingHiveMetastore(hiveDir);
        metastore.createDatabase(Database.builder()
                .setDatabaseName("tpch")
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build());

        HiveConnectorFactory hiveConnectorFactory = new HiveConnectorFactory(
                "hive",
                HiveBenchmarkQueryRunner.class.getClassLoader(),
                metastore);

        Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://none.invalid:0")
                .put("hive.max-split-size", "10GB")
                .build();

        localQueryRunner.createCatalog("hive", hiveConnectorFactory, hiveCatalogConfig);

        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        return localQueryRunner;
    }
}
