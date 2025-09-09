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
package com.facebook.presto.flightshim;

import com.facebook.presto.plugin.singlestore.DockerizedSingleStoreServer;
import com.facebook.presto.plugin.singlestore.SingleStoreQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

@Test
public class TestFlightShimSingleStore
        extends AbstractTestFlightShimQueries
{
    private final DockerizedSingleStoreServer singleStoreServer;

    public TestFlightShimSingleStore()
    {
        this.singleStoreServer = new DockerizedSingleStoreServer();
        closables.add(singleStoreServer);
    }

    @Override
    protected String getConnectorId()
    {
        return "singlestore";
    }

    @Override
    protected String getConnectionUrl()
    {
        return singleStoreServer.getJdbcUrl();
    }

    @Override
    protected String getPluginBundles()
    {
        return "../presto-singlestore/pom.xml";
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return SingleStoreQueryRunner.createSingleStoreQueryRunner(singleStoreServer, ImmutableMap.of(), TpchTable.getTables());
    }
}
