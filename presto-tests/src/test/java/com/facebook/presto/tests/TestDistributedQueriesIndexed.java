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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.tpch.IndexedTpchPlugin;
import com.facebook.presto.tpch.TpchMetadata;
import io.airlift.testing.Closeables;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    private DistributedQueryRunner queryRunner;

    @Override
    protected int getNodeCount()
    {
        return queryRunner.getNodeCount();
    }

    @Override
    protected ConnectorSession setUpQueryFramework()
            throws Exception
    {
        ConnectorSession session = new ConnectorSession("user", "test", "tpch_indexed", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, ENGLISH, null, null);
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 3);

        queryRunner.installPlugin(new IndexedTpchPlugin(getTpchIndexSpec()));
        queryRunner.createCatalog("tpch_indexed", "tpch_indexed");

        this.queryRunner = queryRunner;

        return session;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void tearDownQueryFramework()
            throws Exception
    {
        Closeables.closeQuietly(queryRunner);
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return queryRunner.execute(sql);
    }
}
