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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Set;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.connector.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static com.facebook.presto.connector.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJmxQueries
        extends AbstractTestQueryFramework
{
    private static final Set<String> STANDARD_NAMES = ImmutableSet.<String>builder()
            .add("java.lang:type=ClassLoading")
            .add("java.lang:type=Memory")
            .add("java.lang:type=OperatingSystem")
            .add("java.lang:type=Runtime")
            .add("java.lang:type=Threading")
            .add("java.util.logging:type=Logging")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return JmxQueryRunner.createJmxQueryRunner();
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(INFORMATION_SCHEMA, JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME));
    }

    @Test
    public void testShowTables()
    {
        Set<String> standardNamesLower = STANDARD_NAMES.stream()
                .map(name -> name.toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());
        MaterializedResult result = computeActual("SHOW TABLES");
        assertTrue(result.getOnlyColumnAsSet().containsAll(standardNamesLower));
    }

    @Test
    public void testQuery()
    {
        for (String name : STANDARD_NAMES) {
            computeActual(format("SELECT * FROM \"%s\"", name));
        }
    }

    @Test
    public void testNodeCount()
    {
        String name = STANDARD_NAMES.iterator().next();
        MaterializedResult actual = computeActual("SELECT node_id FROM system.runtime.nodes");
        MaterializedResult expected = computeActual(format("SELECT DISTINCT node FROM \"%s\"", name));
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testOrderOfParametersIsIgnored()
    {
        assertEqualsIgnoreOrder(
                computeActual("SELECT node FROM \"java.nio:type=bufferpool,name=direct\""),
                computeActual("SELECT node FROM \"java.nio:name=direct,type=bufferpool\""));
    }

    @Test
    public void testQueryCumulativeTable()
    {
        computeActual("SELECT * FROM \"*:*\"");
        computeActual("SELECT * FROM \"java.util.logging:*\"");
        assertTrue(computeActual("SELECT * FROM \"java.lang:*\"").getRowCount() > 1);
        assertTrue(computeActual("SELECT * FROM \"jAVA.LANg:*\"").getRowCount() > 1);
    }
}
