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

package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestTableRedirection
{
    @Test
    public void testRedirect()
    {
        Session session = TestingSession.testSessionBuilder().build();
        LocalQueryRunner runner = new LocalQueryRunner(session);
        runner.getMetadata().getTableRedirectionManager().addRule((session1, tableName) -> {
            if (tableName.getCatalogName().equalsIgnoreCase("tpch001") &&
                    tableName.getSchemaName().equalsIgnoreCase("tiny")) {
                return Optional.of(QualifiedObjectName.valueOf("tpch002", "sf1", tableName.getObjectName()));
            }
            return Optional.empty();
        });
        runner.createCatalog("tpch001", new NamingTpchConnectorFactory("tpch001"), ImmutableMap.of());
        runner.createCatalog("tpch002", new NamingTpchConnectorFactory("tpch002"), ImmutableMap.of());

        assertEquals(runner.execute("select count(1) from tpch001.tiny.customer").getOnlyValue(), 150000L);
        assertEquals(runner.execute("select count(1) from tpch002.tiny.customer").getOnlyValue(), 1500L);
        assertEquals(runner.execute("select count(1) from tpch002.sf1.customer").getOnlyValue(), 150000L);
    }

    public static class NamingTpchConnectorFactory
            extends TpchConnectorFactory
    {
        private final String name;

        NamingTpchConnectorFactory(String name)
        {
            super();

            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName()
        {
            return name;
        }
    }
}
