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
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestApproximateQueries
        extends AbstractTestQueries
{
    private final ConnectorSession defaultSampledSession;

    protected AbstractTestApproximateQueries(QueryRunner queryRunner, ConnectorSession defaultSampledSession)
    {
        super(queryRunner);
        this.defaultSampledSession = defaultSampledSession;
    }

    @Test
    public void testApproximateQueryCount()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(*) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(*) FROM orders");
    }

    @Test
    public void testApproximateQueryCountCustkey()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(custkey) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(custkey) FROM orders");
    }

    @Test
    public void testApproximateQuerySum()
            throws Exception
    {
        assertApproximateQuery("SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testApproximateQueryAverage()
            throws Exception
    {
        assertApproximateQuery("SELECT AVG(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT AVG(totalprice) FROM orders");
    }

    private static final Logger log = Logger.get(AbstractTestApproximateQueries.class);

    private void assertApproximateQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActualSampled(actual);
        log.info("FINISHED in %s", Duration.nanosSince(start));

        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTypes());
        assertApproximatelyEqual(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
    }

    private static void assertApproximatelyEqual(List<MaterializedRow> actual, List<MaterializedRow> expected)
            throws Exception
    {
        // TODO: support GROUP BY queries
        assertEquals(actual.size(), 1, "approximate query returned more than one row");

        MaterializedRow actualRow = actual.get(0);
        MaterializedRow expectedRow = expected.get(0);

        for (int i = 0; i < actualRow.getFieldCount(); i++) {
            String actualField = (String) actualRow.getField(i);
            double actualValue = Double.parseDouble(actualField.split(" ")[0]);
            double error = Double.parseDouble(actualField.split(" ")[2]);
            Object expectedField = expectedRow.getField(i);
            assertTrue(expectedField instanceof String || expectedField instanceof Number);
            double expectedValue;
            if (expectedField instanceof String) {
                expectedValue = Double.parseDouble((String) expectedField);
            }
            else {
                expectedValue = ((Number) expectedField).doubleValue();
            }
            assertTrue(Math.abs(actualValue - expectedValue) < error);
        }
    }

    protected MaterializedResult computeActualSampled(@Language("SQL") String sql)
    {
        return queryRunner.execute(defaultSampledSession, sql);
    }
}
