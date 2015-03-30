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
package com.facebook.presto.raptor;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public final class RaptorQueryRunner
{
    private RaptorQueryRunner() {}

    public static QueryRunner createRaptorQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createRaptorQueryRunner(ImmutableList.copyOf(tables));
    }

    public static QueryRunner createRaptorQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 2);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "h2")
                .put("metadata.db.filename", new File(baseDir, "db").getAbsolutePath())
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .put("storage.max-shard-rows", "2000")
                .build();

        queryRunner.createCatalog("default", "raptor", raptorProperties);

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
        copyTpchTables(queryRunner, "tpch_sampled", TINY_SCHEMA_NAME, createSampledSession(), tables);

        return queryRunner;
    }

    public static Session createSession()
    {
        return createSession("tpch");
    }

    public static Session createSampledSession()
    {
        return createSession("tpch_sampled");
    }

    private static Session createSession(String schema)
    {
        return Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("default")
                .setSchema(schema)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }
}
