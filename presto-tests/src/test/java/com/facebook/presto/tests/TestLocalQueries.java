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
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;

public class TestLocalQueries
        extends AbstractTestQueries
{
    public TestLocalQueries()
    {
        super(createLocalQueryRunner());
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        ConnectorSession defaultSession = new ConnectorSession("user", "test", "local", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, null, null);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        return localQueryRunner;
    }
}
