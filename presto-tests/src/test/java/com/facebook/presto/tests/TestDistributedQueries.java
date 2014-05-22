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
package com.facebook.presto.tests;

import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestDistributedQueries
        extends AbstractTestDistributedQueries
{
    private ConnectorSession session;
    private ConnectorSession sampledSession;

    @Override
    public ConnectorSession getSession()
    {
        return session;
    }

    @Override
    public ConnectorSession getSampledSession()
    {
        return sampledSession;
    }

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        session = new ConnectorSession("user", "test", "default", "tpch", UTC_KEY, ENGLISH, null, null);
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 4);

        sampledSession = new ConnectorSession("user", "test", "default", "tpch_sampled", UTC_KEY, ENGLISH, null, null);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new RaptorPlugin());

        // install raptor plugin
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();

        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "h2")
                .put("metadata.db.filename", new File(baseDir, "db").getAbsolutePath())
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .build();

        queryRunner.createCatalog(session.getCatalog(), "raptor", raptorProperties);
        return queryRunner;
    }
}
