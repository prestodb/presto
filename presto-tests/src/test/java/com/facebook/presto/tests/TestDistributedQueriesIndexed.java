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
import com.facebook.presto.tests.tpch.IndexedTpchPlugin;
import com.facebook.presto.tpch.TpchMetadata;
import io.airlift.testing.Closeables;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    public TestDistributedQueriesIndexed()
            throws Exception
    {
        super(createQueryRunner());
    }

    @AfterClass
    public void destroy()
            throws Exception
    {
        Closeables.closeQuietly(queryRunner);
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        ConnectorSession session = new ConnectorSession("user", "test", "tpch_indexed", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, ENGLISH, null, null);
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 3);

        queryRunner.installPlugin(new IndexedTpchPlugin(INDEX_SPEC));
        queryRunner.createCatalog("tpch_indexed", "tpch_indexed");
        return queryRunner;
    }
}
