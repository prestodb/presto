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
import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.apache.hadoop.hive.metastore.api.Database;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public final class HiveQueryRunner
{
    private HiveQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public static QueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }

    public static QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new SampledTpchPlugin());
            queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

            File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
            InMemoryHiveMetastore metastore = new InMemoryHiveMetastore();
            metastore.createDatabase(new Database("tpch", null, new File(baseDir, "tpch").toURI().toString(), null));
            metastore.createDatabase(new Database("tpch_sampled", null, new File(baseDir, "tpch_sampled").toURI().toString(), null));

            queryRunner.installPlugin(new HivePlugin("hive", metastore));
            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .put("hive.metastore.uri", "thrift://localhost:8080")
                    .put("hive.allow-drop-table", "true")
                    .put("hive.allow-rename-table", "true")
                    .build();
            queryRunner.createCatalog("hive", "hive", hiveProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            copyTpchTables(queryRunner, "tpch_sampled", TINY_SCHEMA_NAME, createSampledSession(), tables);

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static Session createSession()
    {
        return createHiveSession(TPCH_SCHEMA);
    }

    public static Session createSampledSession()
    {
        return createHiveSession(TPCH_SAMPLED_SCHEMA);
    }

    private static Session createHiveSession(String schema)
    {
        return new Session("user", "test", "hive", schema, UTC_KEY, ENGLISH, null, null);
    }
}
